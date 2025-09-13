from fast_bitrix24 import Bitrix
import pandas as pd
#import asyncio - при работе в ноутбуке использовался await для метода bx.get_all
import sys

webhook = 'My_webhook'# Здесь заменить на мой вебхук
bx = Bitrix(webhook)

# Выгружаем из системы данные
try:

    print('Выгрузка статусов...')
    statuss_dicts = bx.get_all('crm.status.list')

    print('Выгрузка пользователей...')
    users_dicts = bx.get_all('user.get')

    print('Выгрузка лидов...')
    leads_dicts = bx.get_all('crm.lead.list',
    params={
        'select': ['LAST_NAME', 'NAME','SECOND_NAME','PHONE','EMAIL','COMPANY_TITLE','SOURCE_ID','ID','DATE_CREATE','SOURCE_DESCRIPTION']
        ,"FILTER":{">DATE_CREATE":"2024-09-01"}})
    
    print('Выгрузка сделок...')
    # Чаще всего появляется ошибка при выгрузке, поэтому принято решение замедлить
    with bx.slow(): # Замедление
        deals_dicts = bx.get_all('crm.deal.list',
        params={
        'select': ['TITLE', 'OPPORTUNITY','STAGE_ID','ASSIGNED_BY_ID','LEAD_ID','UF_CRM_MPC17448211751649731412','UF_CRM_MPC17448211751748207566']
        ,"FILTER":{">DATE_CREATE":"2024-09-01"}})

    
# except:
#    sys.exit('Произошла ошибка! Попробуйте подождать или обновите вебхук.')

# Смотрим какая ошибка на самом деле
except Exception as e:
    print(f"Error: {e}")
    sys.exit('Произошла ошибка! Попробуйте подождать или обновите вебхук.')

# Преобразуем данные
print('Преобразование данные')

# Преобразуем пользователей
user = pd.DataFrame(users_dicts)[['ID','NAME','LAST_NAME']]
user['Ответственный'] = user[['LAST_NAME','NAME']].apply(lambda x: ' '.join(list(x.dropna())),axis=1)
user = user.rename(columns={'ID':'ASSIGNED_BY_ID'}).drop(columns=['NAME','LAST_NAME'])

# Преобразуем статусы
status = pd.DataFrame(statuss_dicts)
source = status.query('ENTITY_ID=="SOURCE"' ).rename(columns={'STATUS_ID':'SOURCE_ID','NAME':'Название источника'})[['SOURCE_ID','Название источника']]
stage = status[status['ENTITY_ID'].apply(lambda x: 'DEAL_STAGE' in x)].rename(columns={'STATUS_ID':'STAGE_ID','NAME':'Стадия'})[['STAGE_ID','Стадия']]

# Преобразуем лидов
leads = pd.DataFrame(leads_dicts).merge(source,how='left').drop(columns='SOURCE_ID').rename(
    columns={'LAST_NAME':'Фамилия',
             'NAME':'Имя',
             'SECOND_NAME':'Отчество',
             'COMPANY_TITLE':'Компания',
             'PHONE':'Телефон',
             'DATE_CREATE':'Дата создания',
             'SOURCE_DESCRIPTION':'Форма'
             }
)
leads.loc[~leads['Телефон'].isna(),'Телефон'] = \
    leads.loc[~leads['Телефон'].isna(),'Телефон'].apply(lambda phones: '|'.join([phone['VALUE'] for phone in phones]) if phones else None)
leads.loc[~leads['EMAIL'].isna(),'EMAIL'] = \
    leads.loc[~leads['EMAIL'].isna(),'EMAIL'].apply(lambda emails: '|'.join([email['VALUE'] for email in emails]) if emails else None)

# Преобразуем сделки
deals = pd.DataFrame(deals_dicts).merge(stage,how='left').merge(user,how='left').drop(columns=['STAGE_ID','ID','ASSIGNED_BY_ID']).rename(
    columns={'TITLE':'Название сделки',
             'OPPORTUNITY':'Сумма',
             'UF_CRM_MPC17448211751649731412':'Тип сервиса (2 уровня)',
             'UF_CRM_MPC17448211751748207566':'Тип сервиса (общий)'
             }
)

# Сохраняем данные
print('Сохранение данных')

deals.merge(leads,left_on='LEAD_ID',right_on='ID').drop(columns=['LEAD_ID','Форма']).to_excel('Выгрузка 1.xlsx',index=False)
leads[~leads['Форма'].isna()].to_excel('Выгрузка 2.xlsx',index=False)

print('Выгрузки готовы!')