from DORC_Utils import *
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import math
import prefect
import pendulum

folderpath = '\\\\afs\\Areas\\Tesouraria-Corporativa\\Compartilhado\\Ruth Menezes\\DORC\\tesouraria_corporativa\\'

schedule = BBM_Schedule('*/1 10-18 * * *', start_date=pendulum.now('America/Sao_Paulo'))  # 10h30 - 17h30
# schedule = BBM_Schedule().every().minute() # 10h30 - 17h30

# print(list(map(lambda time: time.strftime('%HH%MM'), schedule.next(1000))))

@task()
def get_date():
    return datetime.now().strftime('%Y%m%d')

@task()
def consulta_fig(pub: pd.DataFrame):
    pub = pub.drop(['Data Referência', 'Agrupador', 'Boleto'], axis=1)
    pub = pub[pub['Vencimento'].notna()]
    pub['Tipo'] = pub.apply(lambda row: 'POS' if row['Taxa Cliente'] > 0.9 else 'PRE', axis=1)
    pub['Vencimento'] = pd.to_datetime(pub['Vencimento'])
    pub['Emissão'] = pd.to_datetime(pub['Emissão'])
    pub['Prazo'] = pub.apply(lambda row: np.around(
        np.around((row['Vencimento']-row['Emissão']).days/30)/3, 0)*3, axis=1)
    return pub


@task()
def consulta_tesouraria(pub: pd.DataFrame):
    pub = pub.drop(['Data Referência', 'IPCA +'], axis=1)
    pub = pub[pub['Vencimento'].notna()]
    pub['Fixa'] = pub.apply(lambda row: np.around(row['Fixa'], 4), axis=1)
    pub['Referência'] = pub.apply(lambda row: np.around(row['Referência'], 4), axis=1)
    return pub


def nwdays(d1, d2, workdays):
    return workdays.index(d2.strftime("%Y-%m-%d")) - workdays.index(d1.strftime("%Y-%m-%d"))


@task()
def publicacao_fig(fig: pd.DataFrame, tes: pd.DataFrame, workdays):
    # Power Query
    pub = fig.merge(tes, how='left', left_on=['Produto', 'Prazo'], right_on=[
                    'Produto', 'Prazo (meses)'], suffixes=('', '_tes'))
    pub = pub.rename({'Taxa Distribuidor': 'Taxa Distribuidor_FIG'}, axis=1)
    pub['Taxa Distribuidor'] = pub.apply(lambda row:
                                         (row['Taxa Distribuidor_FIG'] or row['Referência']) if row['Taxa Cliente'] > 0.5 else
                                         (row['Taxa Distribuidor_FIG'] or row['Fixa']), axis=1)
    pub['PU'] = pub['PU'].fillna(0)

    pub = pub.loc[:, ['Emissão', 'Vencimento', 'Taxa Distribuidor', 'DI Ref', 'Taxa Cliente', 'PU',
                      'Quantidade', 'Contraparte', 'Produto', 'Tipo', 'Prazo', 'Fixa', 'DI1', 'Referência']]

    #Excel Formulas

    pub['DI_ref_POS'] = pub['DI1']
    pub['Taxa_Dis_pos'] = pub.apply(lambda row: 0 if row['Tipo'] == "PRE" else math.pow(
        ((math.pow(1+row['DI_ref_POS'], 1/252))-1)*row['Taxa Distribuidor']+1, 252)-1, axis=1)
    pub['Taxa_Cli_pos'] = pub.apply(lambda row: 0 if row['Tipo'] == "PRE" else math.pow(
        ((math.pow(1+row['DI_ref_POS'], 1/252))-1)*row['Taxa Cliente']+1, 252)-1, axis=1)

    pub['Batimento_taxa'] = pub.apply(lambda row:
                                      ("TRUE" if row['Fixa'] >= row['Taxa Distribuidor'] else "FALSE") if row['Tipo'] == "PRE" else
                                      ("TRUE" if row['Referência'] >= row['Taxa Distribuidor'] else "FALSE"), axis=1)

    workdays = workdays.strftime('%Y-%m-%d').to_list()
    pub['nwdays'] = pub.apply(lambda row: nwdays(row['Emissão'], row['Vencimento'], workdays), axis=1)
    pub['PU_PRE'] = pub.apply(lambda row: 0 if row['Tipo'] == "POS" else np.around((math.pow(
        1+row['Taxa Cliente'], row['nwdays']/252)/math.pow(1+row['Taxa Distribuidor'], row['nwdays']/252))*1000, 8), axis=1)
    pub['PU_POS'] = pub.apply(lambda row: 0 if row['Tipo'] == "PRE" else np.around(
        (math.pow(1+row['Taxa_Cli_pos'], row['nwdays']/252)/math.pow(1+row['Taxa_Dis_pos'], row['nwdays']/252))*1000, 8), axis=1)

    pub = pub.loc[:, ['Emissão', 'Vencimento', 'Taxa Distribuidor', 'DI Ref', 'Taxa Cliente', 'PU', 'Quantidade', 'Contraparte',
                      'Produto', 'Tipo', 'Prazo', 'Batimento_taxa', 'PU_PRE', 'DI_ref_POS', 'Taxa_Dis_pos', 'Taxa_Cli_pos', 'PU_POS']]

    pub['Diferença'] = pub.apply(lambda row: 0 if row['PU'] == 0 else np.around(
        (row['PU_' + row['Tipo']]-row['PU'])/row['PU'], 10), axis=1)

    #Power Query
    pub['PU_tesouraria'] = pub.apply(lambda row: row['PU_PRE'] if row['PU_PRE'] != 0 else row['PU_POS'], axis=1)
    pub['Batimento PU'] = pub.apply(lambda row: ('OK' if abs(row['Diferença']) <
                                    0.0005 else 'REVALIDAR') if row['PU'] else 'OK', axis=1)
    pub = pub.rename({'DI_ref_POS': 'DI_ref', 'Tipo': 'Indexador'}, axis=1)
    pub = pub.loc[:, ['Contraparte', 'Emissão', 'Vencimento', 'Taxa Distribuidor', 'Taxa Cliente',
                      'DI_ref', 'PU_tesouraria', 'Quantidade', 'Prazo', 'Produto', 'Indexador', 'Batimento PU']]

    return pub


@task()
def save_raw(df: pd.DataFrame, today):
    df.to_csv(folderpath + 'raw.csv', index=False, sep=';', decimal=',')
    df.to_csv(folderpath + 'old\\' + 'raw_' + today + '.csv', index=False, sep=';', decimal=',')


@task()
def save_temp(df: pd.DataFrame, name):
    df.to_csv(folderpath + name + '.csv', index=False, sep=';', decimal=',')


def same(df1, df2):
    columns = df1.columns
    for column in columns:
        if column[:2] != '__' and not df1[column].fillna('').equals(df2[column].fillna('')):
            return False
    return True


@task()
def updated(new_pub_399, new_pub_740):
    old_pub_399 = pd.read_csv(folderpath + 'pub_399.csv', sep=';', decimal=',')
    old_pub_740 = pd.read_csv(folderpath + 'pub_740.csv', sep=';', decimal=',')

    if same(old_pub_399, new_pub_399) and same(old_pub_740, new_pub_740):
        return False
    else:
        return True


with BBM_Flow('unit price check', schedule=schedule) as flow:
    today = get_date()
    yesterday = (datetime.now() - timedelta(1)).strftime('%Y%m%d')
    pub_399 = read_publicador(399, data_base=yesterday)
    pub_740 = read_publicador(740, data_base=yesterday)

    with prefect.case(updated(pub_399, pub_740), True):
        saved_399 = save_temp(pub_399, 'pub_399')
        saved_740 = save_temp(pub_740, 'pub_740')

        workdays = get_workdays('2000-01-01', '2100-01-01')

        res_fig = consulta_fig(pub_399)
        res_tes = consulta_tesouraria(pub_740)

        pub_fig = publicacao_fig(res_fig, res_tes, workdays)

        print(pub_fig)

        save_raw(pub_fig, today)

flow.run()
