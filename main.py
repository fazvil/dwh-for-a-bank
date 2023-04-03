#!/usr/bin/python3

import psycopg2
import pandas as pd
import os

# Создание подключения к источнику
conn_src = psycopg2.connect(database = "bank",
                            host =     "de-edu-db.chronosavant.ru",
                            user =     "bank_etl",
                            password = "bank_etl_password",
                            port =     "5432")

# Создание подключения к хранилищу
conn_dwh = psycopg2.connect(database = "edu",
                            host =     "de-edu-db.chronosavant.ru",
                            user =     "de10",
                            password = "bilbobaggins",
                            port =     "5432")

# Отключение автокоммита
conn_src.autocommit = False
conn_dwh.autocommit = False

# Создание курсора
cursor_src = conn_src.cursor()
cursor_dwh = conn_dwh.cursor()

####################################################
## Выделяем дату отчета

for f in os.listdir():
    if f[:9] == 'terminals':
        report_dt = f[10:18]


####################################################
## Очистка стейджинга

cursor_dwh.execute("delete from de10.fzlv_stg_accounts")
cursor_dwh.execute("delete from de10.fzlv_stg_cards")
cursor_dwh.execute("delete from de10.fzlv_stg_clients")
cursor_dwh.execute("delete from de10.fzlv_stg_transactions")
cursor_dwh.execute("delete from de10.fzlv_stg_terminals")
cursor_dwh.execute("delete from de10.fzlv_stg_blacklist")

cursor_dwh.execute("delete from de10.fzlv_stg_del_accounts")
cursor_dwh.execute("delete from de10.fzlv_stg_del_cards")
cursor_dwh.execute("delete from de10.fzlv_stg_del_clients")
cursor_dwh.execute("delete from de10.fzlv_stg_del_terminals")

####################################################
## Захват данных из источника (измененных с момента последней загрузки) в стейджинг
####################################################

# stg_accounts

cursor_dwh.execute( """ select to_char(max_update_dt, 'YYYY-MM-DD HH24:MI:SS')
                            from fzlv_meta_max_update_dt
                            where schema_name = 'de10'
                            and table_name = 'fzlv_accounts' """)
records = cursor_dwh.fetchone()

cursor_src.execute( f""" select
                            account,
                            valid_to,
                            client,
                            create_dt,
                            update_dt
                        from info.accounts
                        where (
                            update_dt is null
                            and create_dt > to_timestamp('{records[0][0]}', 'YYYY-MM-DD HH24:MI:SS')
                        ) or update_dt > to_timestamp('{records[0][0]}', 'YYYY-MM-DD HH24:MI:SS') """ )  
                        
records = cursor_src.fetchall()

names = [ x[0] for x in cursor_src.description ]
df = pd.DataFrame( records, columns = names )

cursor_dwh.executemany( """ insert into de10.fzlv_stg_accounts (
                                account,
                                valid_to,
                                client,
                                create_dt,
                                update_dt
                            ) VALUES( %s, %s, %s, %s, %s )""", df.values.tolist() )

# stg_cards

cursor_dwh.execute( """ select to_char(max_update_dt, 'YYYY-MM-DD HH24:MI:SS')
                            from fzlv_meta_max_update_dt
                            where schema_name = 'de10'
                            and table_name = 'fzlv_cards' """)
records = cursor_dwh.fetchone()

cursor_src.execute( f""" select
                            card_num,
                            account,
							create_dt,
							update_dt
                        from info.cards
                        where (
                            update_dt is null
                            and create_dt > to_timestamp('{records[0][0]}', 'YYYY-MM-DD HH24:MI:SS')
                        ) or update_dt > to_timestamp('{records[0][0]}', 'YYYY-MM-DD HH24:MI:SS') """ )  
                        
records = cursor_src.fetchall()

names = [ x[0] for x in cursor_src.description ]
df = pd.DataFrame( records, columns = names )

cursor_dwh.executemany( """ insert into de10.fzlv_stg_cards (
                                card_num,
								account,
								create_dt,
								update_dt
                            ) VALUES( %s, %s, %s, %s )""", df.values.tolist() )

# stg_clients

cursor_dwh.execute( """ select to_char(max_update_dt, 'YYYY-MM-DD HH24:MI:SS')
                            from fzlv_meta_max_update_dt
                            where schema_name = 'de10'
                            and table_name = 'fzlv_clients' """)
records = cursor_dwh.fetchone()

cursor_src.execute( f""" select
                            client_id,
							last_name,
							first_name,
							patronymic,
							date_of_birth,
							passport_num,
							passport_valid_to,
							phone,
							create_dt,
							update_dt
                        from info.clients
                        where (
                            update_dt is null
                            and create_dt > to_timestamp('{records[0][0]}', 'YYYY-MM-DD HH24:MI:SS')
                        ) or update_dt > to_timestamp('{records[0][0]}', 'YYYY-MM-DD HH24:MI:SS') """ )  
                        
records = cursor_src.fetchall()

names = [ x[0] for x in cursor_src.description ]
df = pd.DataFrame( records, columns = names )

cursor_dwh.executemany( """ insert into de10.fzlv_stg_clients (
                                client_id,
								last_name,
								first_name,
								patronymic,
								date_of_birth,
								passport_num,
								passport_valid_to,
								phone,
								create_dt,
								update_dt
                            ) VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )""", df.values.tolist() )

# stg_terminals

df_terminals = pd.read_excel( '/home/de10/fzlv/project/terminals_' + report_dt + '.xlsx', sheet_name='terminals', header=0, index_col=None )

cursor_dwh.executemany( """ insert into de10.fzlv_stg_terminals (
                                terminal_id,
                                terminal_type,
                                terminal_city,
                                terminal_address
                        ) VALUES( %s, %s, %s, %s ) """, df_terminals.values.tolist() )

# stg_transactions

df = pd.read_csv( '/home/de10/fzlv/project/transactions_' + report_dt + '.txt',sep = ";" )

cursor_dwh.executemany( """ insert into de10.fzlv_stg_transactions (
                                transaction_id,
                                transaction_date,
                                amount,
                                card_num,
                                oper_type,
                                oper_result,
                                terminal
                            ) VALUES( %s, %s, %s, %s, %s, %s, %s )""", df.values.tolist() )

# stg_blacklist

df = pd.read_excel( '/home/de10/fzlv/project/passport_blacklist_' + report_dt + '.xlsx', sheet_name='blacklist', header=0, index_col=None )

cursor_dwh.executemany( """ insert into de10.fzlv_stg_blacklist (
                                entry_dt,
                                passport_num
                        ) VALUES( %s, %s )""", df.values.tolist() )


####################################################
## Захват в стейджинг ключей из источника полным срезом для вычисления удалений
####################################################

# stg_del_accounts
cursor_src.execute( " select account from info.accounts " )
records = cursor_src.fetchall()
names = [ x[0] for x in cursor_src.description ]
df = pd.DataFrame( records, columns = names )
cursor_dwh.executemany( " insert into de10.fzlv_stg_del_accounts (account_num) VALUES( %s ) ", df.values.tolist() )

# stg_del_cards
cursor_src.execute( " select card_num from info.cards " )
records = cursor_src.fetchall()
names = [ x[0] for x in cursor_src.description ]
df = pd.DataFrame( records, columns = names )
cursor_dwh.executemany( " insert into de10.fzlv_stg_del_cards (card_num) VALUES( %s ) ", df.values.tolist() )

# stg_del_clients
cursor_src.execute( " select client_id from info.clients " )
records = cursor_src.fetchall()
names = [ x[0] for x in cursor_src.description ]
df = pd.DataFrame( records, columns = names )
cursor_dwh.executemany( " insert into de10.fzlv_stg_del_clients (client_id) VALUES( %s ) ", df.values.tolist() )

# stg_del_terminals
cursor_dwh.execute( " select terminal_id from de10.fzlv_stg_terminals " )
records = cursor_dwh.fetchall()
names = [ x[0] for x in cursor_src.description ]
df = pd.DataFrame( records, columns = names )
cursor_dwh.executemany( " insert into de10.fzlv_stg_del_terminals ( terminal_id ) VALUES( %s )", df.values.tolist() )

####################################################
## Загрузка в приемник (формат SCD2)
####################################################

####################################################
# dwh_dim_account
# Загрузка вставок dwh_dim_accounts

cursor_dwh.execute( """ insert into de10.fzlv_dwh_dim_accounts_hist (
                            account_num,
                            valid_to,
                            client,
                            effective_from,
                            effective_to,
                            deleted_flg
                        )
                        select
                            stg.account,
                            stg.valid_to,
                            stg.client,
                            coalesce( stg.update_dt, stg.create_dt),
                            to_date( '9999-12-31', 'YYYY-MM-DD' ),
                            'N'
                        from de10.fzlv_stg_accounts stg
                        left join de10.fzlv_dwh_dim_accounts_hist tgt
                        on stg.account = tgt.account_num
                            and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                            and tgt.deleted_flg = 'N'
                        where tgt.account_num is null """)

# Обновление в приемнике "обновлений" на источнике dwh_dim_accounts

cursor_dwh.execute( """ update de10.fzlv_dwh_dim_accounts_hist
                        set
                            effective_to = tmp.update_dt - interval '1 day'
                        from (
                            select
                                stg.account,
                                stg.update_dt
                            from de10.fzlv_stg_accounts stg
                            inner join de10.fzlv_dwh_dim_accounts_hist tgt
                            on stg.account = tgt.account_num
                                and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                                and tgt.deleted_flg = 'N'
                            where 0=1
                                or stg.valid_to <> tgt.valid_to or (stg.valid_to is null and tgt.valid_to is not null) or (stg.valid_to is not null and tgt.valid_to is null)
                                or stg.client <> tgt.client or (stg.client is null and tgt.client is not null) or (stg.client is not null and tgt.client is null)
                        ) tmp
                        where de10.fzlv_dwh_dim_accounts_hist.account_num = tmp.account
                        and de10.fzlv_dwh_dim_accounts_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                        and de10.fzlv_dwh_dim_accounts_hist.deleted_flg = 'N' """ )

cursor_dwh.execute( """ insert into de10.fzlv_dwh_dim_accounts_hist (
                            account_num,
                            valid_to,
                            client,
                            effective_from,
                            effective_to,
                            deleted_flg
                        )
                        select
                            stg.account,
                            stg.valid_to,
                            stg.client,
                            stg.update_dt,
                            to_date( '9999-12-31', 'YYYY-MM-DD' ),
                            'N'
                        from de10.fzlv_stg_accounts stg
                        inner join de10.fzlv_dwh_dim_accounts_hist tgt
                        on stg.account = tgt.account_num
                            and tgt.effective_to = stg.update_dt - interval '1 day'
                            and tgt.deleted_flg = 'N'
                        where 0=1
                            or stg.valid_to <> tgt.valid_to or (stg.valid_to is null and tgt.valid_to is not null) or (stg.valid_to is not null and tgt.valid_to is null)
                            or stg.client <> tgt.client or (stg.client is null and tgt.client is not null) or (stg.client is not null and tgt.client is null) """)

# Удаление в приемнике удаленных в источнике записей dwh_dim_accounts

cursor_dwh.execute( """ insert into de10.fzlv_dwh_dim_accounts_hist (
                            account_num,
                            valid_to,
                            client,
                            effective_from,
                            effective_to,
                            deleted_flg
                        )
                        select
                            tgt.account_num,
                            tgt.valid_to,
                            tgt.client,
                            now(),
                            to_date( '9999-12-31', 'YYYY-MM-DD' ),
                            'Y'
                        from de10.fzlv_dwh_dim_accounts_hist tgt
                        where tgt.account_num in (
                            select tgt.account_num
                            from de10.fzlv_dwh_dim_accounts_hist tgt
                            left join de10.fzlv_stg_del_accounts stg
                            on stg.account_num = tgt.account_num
                            where stg.account_num is null
                                and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                                and tgt.deleted_flg = 'N'                            
                        )
                        and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                        and tgt.deleted_flg = 'N' """ )

cursor_dwh.execute( """ update de10.fzlv_dwh_dim_accounts_hist
                        set effective_to = now() - interval '1 day'
                        where de10.fzlv_dwh_dim_accounts_hist.account_num in (
                            select tgt.account_num
                            from de10.fzlv_dwh_dim_accounts_hist tgt
                            left join de10.fzlv_stg_del_accounts stg
                            on stg.account_num = tgt.account_num
                            where stg.account_num is null
                                and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                                and tgt.deleted_flg = 'N' 
                            )
                        and de10.fzlv_dwh_dim_accounts_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                        and de10.fzlv_dwh_dim_accounts_hist.deleted_flg = 'N' """ )

####################################################
# dwh_dim_cards
# Загрузка вставок dwh_dim_cards

cursor_dwh.execute( """ insert into de10.fzlv_dwh_dim_cards_hist (
                            card_num,
                            account_num,
                            effective_from,
                            effective_to,
                            deleted_flg
                        )
                        select
                            stg.card_num,
                            stg.account,
                            coalesce( stg.update_dt, stg.create_dt),
                            to_date( '9999-12-31', 'YYYY-MM-DD' ),
                            'N'
                        from de10.fzlv_stg_cards stg
                        left join de10.fzlv_dwh_dim_cards_hist tgt
                        on stg.card_num = tgt.card_num
                            and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                            and tgt.deleted_flg = 'N'
                        where tgt.card_num is null """)

# Обновление в приемнике "обновлений" на источнике dwh_dim_cards

cursor_dwh.execute( """ update de10.fzlv_dwh_dim_cards_hist
                        set
                            effective_to = tmp.update_dt - interval '1 day'
                        from (
                            select
                                stg.card_num,
                                stg.update_dt
                            from de10.fzlv_stg_cards stg
                            inner join de10.fzlv_dwh_dim_cards_hist tgt
                            on stg.card_num = tgt.card_num
                                and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                                and tgt.deleted_flg = 'N'
                            where 0=1
                                or stg.account <> tgt.account_num or (stg.account is null and tgt.account_num is not null) or (stg.account is not null and tgt.account_num is null)
                        ) tmp
                        where de10.fzlv_dwh_dim_cards_hist.card_num = tmp.card_num
                            and de10.fzlv_dwh_dim_cards_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                            and de10.fzlv_dwh_dim_cards_hist.deleted_flg = 'N' """ )

cursor_dwh.execute( """ insert into de10.fzlv_dwh_dim_cards_hist (
                            card_num,
                            account_num,
                            effective_from,
                            effective_to,
                            deleted_flg
                        )
                        select
                            stg.card_num,
                            stg.account,
                            stg.update_dt,
                            to_date( '9999-12-31', 'YYYY-MM-DD' ),
                            'N'
                        from de10.fzlv_stg_cards stg
                        inner join de10.fzlv_dwh_dim_cards_hist tgt
                        on stg.card_num = tgt.card_num
                            and tgt.effective_to = stg.update_dt - interval '1 day'
                            and tgt.deleted_flg = 'N'
                        where 0=1
                            or stg.account <> tgt.account_num or (stg.account is null and tgt.account_num is not null) or (stg.account is not null and tgt.account_num is null) """ )

# Удаление в приемнике удаленных в источнике записей dwh_dim_cards

cursor_dwh.execute( """ insert into de10.fzlv_dwh_dim_cards_hist (
                            card_num,
                            account_num,
                            effective_from,
                            effective_to,
                            deleted_flg
                        )
                        select
                            tgt.card_num,
                            tgt.account_num,
                            now(),
                            to_date( '9999-12-31', 'YYYY-MM-DD' ),
                            'Y'
                        from de10.fzlv_dwh_dim_cards_hist tgt
                        where tgt.card_num in (
                            select tgt.card_num
                            from de10.fzlv_dwh_dim_cards_hist tgt
                            left join de10.fzlv_stg_del_cards stg
                            on stg.card_num = tgt.card_num
                            where stg.card_num is null
                                and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                                and tgt.deleted_flg = 'N'                            
                        )
                        and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                        and tgt.deleted_flg = 'N' """ )

cursor_dwh.execute( """ update de10.fzlv_dwh_dim_cards_hist
                        set effective_to = now() - interval '1 day'
                        where de10.fzlv_dwh_dim_cards_hist.card_num in (
                            select tgt.card_num
                            from de10.fzlv_dwh_dim_cards_hist tgt
                            left join de10.fzlv_stg_del_cards stg
                            on stg.card_num = tgt.card_num
                            where stg.card_num is null
                                and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                                and tgt.deleted_flg = 'N' 
                            )
                        and de10.fzlv_dwh_dim_cards_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                        and de10.fzlv_dwh_dim_cards_hist.deleted_flg = 'N' """ )

####################################################
# dwh_dim_clients
# Загрузка вставок dwh_dim_clients

cursor_dwh.execute( """ insert into de10.fzlv_dwh_dim_clients_hist (
                            client_id,
                            last_name,
                            first_name,
                            patronymic,
                            date_of_birth,
                            passport_num,
                            passport_valid_to,
                            phone,
                            effective_from,
                            effective_to,
                            deleted_flg
                        )
                        select
                            stg.client_id,
                            stg.last_name,
                            stg.first_name,
                            stg.patronymic,
                            stg.date_of_birth,
                            stg.passport_num,
                            stg.passport_valid_to,
                            stg.phone,
                            coalesce( stg.update_dt, stg.create_dt),
                            to_date( '9999-12-31', 'YYYY-MM-DD' ),
                            'N'
                        from de10.fzlv_stg_clients stg
                        left join de10.fzlv_dwh_dim_clients_hist tgt
                        on stg.client_id = tgt.client_id
                            and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                            and tgt.deleted_flg = 'N'
                        where tgt.client_id is null """)

# Обновление в приемнике "обновлений" на источнике dwh_dim_clients

cursor_dwh.execute( """ update de10.fzlv_dwh_dim_clients_hist
                        set
                            effective_to = tmp.update_dt - interval '1 day'
                        from (
                            select
                                stg.client_id,
                                stg.update_dt
                            from de10.fzlv_stg_clients stg
                            inner join de10.fzlv_dwh_dim_clients_hist tgt
                            on stg.client_id = tgt.client_id
                                and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                                and tgt.deleted_flg = 'N'
                            where 0=1
                                or stg.last_name <> tgt.last_name or (stg.last_name is null and tgt.last_name is not null) or (stg.last_name is not null and tgt.last_name is null)
                                or stg.first_name <> tgt.first_name or (stg.first_name is null and tgt.first_name is not null) or (stg.first_name is not null and tgt.first_name is null)
                                or stg.patronymic <> tgt.patronymic or (stg.patronymic is null and tgt.patronymic is not null) or (stg.patronymic is not null and tgt.patronymic is null)
                                or stg.date_of_birth <> tgt.date_of_birth or (stg.date_of_birth is null and tgt.date_of_birth is not null) or (stg.date_of_birth is not null and tgt.date_of_birth is null)
                                or stg.passport_num <> tgt.passport_num or (stg.passport_num is null and tgt.passport_num is not null) or (stg.passport_num is not null and tgt.passport_num is null)
                                or stg.passport_valid_to <> tgt.passport_valid_to or (stg.passport_valid_to is null and tgt.passport_valid_to is not null) or (stg.passport_valid_to is not null and tgt.passport_valid_to is null)
                                or stg.phone <> tgt.phone or (stg.phone is null and tgt.phone is not null) or (stg.phone is not null and tgt.phone is null)      
                        ) tmp
                        where de10.fzlv_dwh_dim_clients_hist.client_id = tmp.client_id
                            and de10.fzlv_dwh_dim_clients_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                            and de10.fzlv_dwh_dim_clients_hist.deleted_flg = 'N' """ )

cursor_dwh.execute( """ insert into de10.fzlv_dwh_dim_clients_hist (
                            client_id,
                            last_name,
                            first_name,
                            patronymic,
                            date_of_birth,
                            passport_num,
                            passport_valid_to,
                            phone,
                            effective_from,
                            effective_to,
                            deleted_flg
                        )
                        select
                            stg.client_id,
                            stg.last_name,
                            stg.first_name,
                            stg.patronymic,
                            stg.date_of_birth,
                            stg.passport_num,
                            stg.passport_valid_to,
                            stg.phone,
                            stg.update_dt,
                            to_date( '9999-12-31', 'YYYY-MM-DD' ),
                            'N'
                        from de10.fzlv_stg_clients stg
                        inner join de10.fzlv_dwh_dim_clients_hist tgt
                        on stg.client_id = tgt.client_id
                            and tgt.effective_to = stg.update_dt - interval '1 day'
                            and tgt.deleted_flg = 'N'
                        where 0=1
                            or stg.last_name <> tgt.last_name or (stg.last_name is null and tgt.last_name is not null) or (stg.last_name is not null and tgt.last_name is null)
                                or stg.first_name <> tgt.first_name or (stg.first_name is null and tgt.first_name is not null) or (stg.first_name is not null and tgt.first_name is null)
                                or stg.patronymic <> tgt.patronymic or (stg.patronymic is null and tgt.patronymic is not null) or (stg.patronymic is not null and tgt.patronymic is null)
                                or stg.date_of_birth <> tgt.date_of_birth or (stg.date_of_birth is null and tgt.date_of_birth is not null) or (stg.date_of_birth is not null and tgt.date_of_birth is null)
                                or stg.passport_num <> tgt.passport_num or (stg.passport_num is null and tgt.passport_num is not null) or (stg.passport_num is not null and tgt.passport_num is null)
                                or stg.passport_valid_to <> tgt.passport_valid_to or (stg.passport_valid_to is null and tgt.passport_valid_to is not null) or (stg.passport_valid_to is not null and tgt.passport_valid_to is null)
                                or stg.phone <> tgt.phone or (stg.phone is null and tgt.phone is not null) or (stg.phone is not null and tgt.phone is null)  """ )

# Удаление в приемнике удаленных в источнике записей dwh_dim_clients

cursor_dwh.execute( """ insert into de10.fzlv_dwh_dim_clients_hist (
                            client_id,
                            last_name,
                            first_name,
                            patronymic,
                            date_of_birth,
                            passport_num,
                            passport_valid_to,
                            phone,
                            effective_from,
                            effective_to,
                            deleted_flg
                        )
                        select
                            tgt.client_id,
                            tgt.last_name,
                            tgt.first_name,
                            tgt.patronymic,
                            tgt.date_of_birth,
                            tgt.passport_num,
                            tgt.passport_valid_to,
                            tgt.phone,
                            now(),
                            to_date( '9999-12-31', 'YYYY-MM-DD' ),
                            'Y'
                        from de10.fzlv_dwh_dim_clients_hist tgt
                        where tgt.client_id in (
                            select tgt.client_id
                            from de10.fzlv_dwh_dim_clients_hist tgt
                            left join de10.fzlv_stg_del_clients stg
                            on stg.client_id = tgt.client_id
                            where stg.client_id is null
                                and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                                and tgt.deleted_flg = 'N'                            
                        )
                        and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                        and tgt.deleted_flg = 'N' """ )

cursor_dwh.execute( """ update de10.fzlv_dwh_dim_clients_hist
                        set effective_to = now() - interval '1 day'
                        where de10.fzlv_dwh_dim_clients_hist.client_id in (
                            select tgt.client_id
                            from de10.fzlv_dwh_dim_clients_hist tgt
                            left join de10.fzlv_stg_del_clients stg
                            on stg.client_id = tgt.client_id
                            where stg.client_id is null
                                and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                                and tgt.deleted_flg = 'N' 
                            )
                        and de10.fzlv_dwh_dim_clients_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                        and de10.fzlv_dwh_dim_clients_hist.deleted_flg = 'N' """ )

####################################################
# dwh_dim_terminals
# Загрузка вставок dwh_dim_terminals

cursor_dwh.execute( f""" insert into de10.fzlv_dwh_dim_terminals_hist (
                            terminal_id,
                            terminal_type,
                            terminal_city,
                            terminal_address,
                            effective_from,
                            effective_to,
                            deleted_flg
                        )
                        select
                            stg.terminal_id,
                            stg.terminal_type,
                            stg.terminal_city,
                            stg.terminal_address,
                            to_date('{report_dt}', 'DDMMYYYY'),
                            to_date( '9999-12-31', 'YYYY-MM-DD' ),
                            'N'
                        from de10.fzlv_stg_terminals stg
                        left join de10.fzlv_dwh_dim_terminals_hist tgt
                        on stg.terminal_id = tgt.terminal_id
                            and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                            and tgt.deleted_flg = 'N'
                        where tgt.terminal_id is null """)

# Обновление в приемнике "обновлений" на источнике dwh_dim_terminals

cursor_dwh.execute( f""" update de10.fzlv_dwh_dim_terminals_hist
                        set
                            effective_to = to_date('{report_dt}', 'DDMMYYYY') - interval '1 day'
                        from (
                            select
                                stg.terminal_id
                            from de10.fzlv_stg_terminals stg
                            inner join de10.fzlv_dwh_dim_terminals_hist tgt
                            on stg.terminal_id = tgt.terminal_id
                                and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                                and tgt.deleted_flg = 'N'
                            where 0=1
                                or stg.terminal_type <> tgt.terminal_type or (stg.terminal_type is null and tgt.terminal_type is not null) or (stg.terminal_type is not null and tgt.terminal_type is null)
                                or stg.terminal_city <> tgt.terminal_city or (stg.terminal_city is null and tgt.terminal_city is not null) or (stg.terminal_city is not null and tgt.terminal_city is null)
                                or stg.terminal_address <> tgt.terminal_address or (stg.terminal_address is null and tgt.terminal_address is not null) or (stg.terminal_address is not null and tgt.terminal_address is null)  
                        ) tmp
                        where de10.fzlv_dwh_dim_terminals_hist.terminal_id = tmp.terminal_id
                            and de10.fzlv_dwh_dim_terminals_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                            and de10.fzlv_dwh_dim_terminals_hist.deleted_flg = 'N' """ )

cursor_dwh.execute( f""" insert into de10.fzlv_dwh_dim_terminals_hist (
                            terminal_id,
                            terminal_type,
                            terminal_city,
                            terminal_address,
                            effective_from,
                            effective_to,
                            deleted_flg
                        )
                        select
                            stg.terminal_id,
                            stg.terminal_type,
                            stg.terminal_city,
                            stg.terminal_address,
                            to_date( '{report_dt}', 'DDMMYYYY' ),
                            to_date( '9999-12-31', 'YYYY-MM-DD' ),
                            'N'
                        from de10.fzlv_stg_terminals stg
                        inner join de10.fzlv_dwh_dim_terminals_hist tgt
                        on stg.terminal_id = tgt.terminal_id
                            and tgt.effective_to = to_date('{report_dt}', 'DDMMYYYY') - interval '1 day'
                            and tgt.deleted_flg = 'N'
                        where 0=1
                            or stg.terminal_type <> tgt.terminal_type or (stg.terminal_type is null and tgt.terminal_type is not null) or (stg.terminal_type is not null and tgt.terminal_type is null)
                            or stg.terminal_city <> tgt.terminal_city or (stg.terminal_city is null and tgt.terminal_city is not null) or (stg.terminal_city is not null and tgt.terminal_city is null)
                            or stg.terminal_address <> tgt.terminal_address or (stg.terminal_address is null and tgt.terminal_address is not null) or (stg.terminal_address is not null and tgt.terminal_address is null)   """ )

# Удаление в приемнике удаленных в источнике записей dwh_dim_terminals

cursor_dwh.execute( f""" insert into de10.fzlv_dwh_dim_terminals_hist (
                            terminal_id,
                            terminal_type,
                            terminal_city,
                            terminal_address,
                            effective_from,
                            effective_to,
                            deleted_flg
                        )
                        select
                            tgt.terminal_id,
                            tgt.terminal_type,
                            tgt.terminal_city,
                            tgt.terminal_address,
                            to_date( '{report_dt}', 'DDMMYYYY' ),
                            to_date( '9999-12-31', 'YYYY-MM-DD' ),
                            'Y'
                        from de10.fzlv_dwh_dim_terminals_hist tgt
                        where tgt.terminal_id in (
                            select tgt.terminal_id
                            from de10.fzlv_dwh_dim_terminals_hist tgt
                            left join de10.fzlv_stg_del_terminals stg
                            on stg.terminal_id = tgt.terminal_id
                            where stg.terminal_id is null
                                and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                                and tgt.deleted_flg = 'N'                            
                        )
                        and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                        and tgt.deleted_flg = 'N' """ )

cursor_dwh.execute( f""" update de10.fzlv_dwh_dim_terminals_hist
                        set effective_to = to_date('{report_dt}', 'DDMMYYYY') - interval '1 day'
                        where de10.fzlv_dwh_dim_terminals_hist.terminal_id in (
                            select tgt.terminal_id
                            from de10.fzlv_dwh_dim_terminals_hist tgt
                            left join de10.fzlv_stg_del_terminals stg
                            on stg.terminal_id = tgt.terminal_id
                            where stg.terminal_id is null
                                and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                                and tgt.deleted_flg = 'N' 
                            )
                        and de10.fzlv_dwh_dim_terminals_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                        and de10.fzlv_dwh_dim_terminals_hist.deleted_flg = 'N' """ )


####################################################
## Обновление метаданных
####################################################

# Обновление метаданных fzlv_accounts

cursor_dwh.execute( """ update de10.fzlv_meta_max_update_dt
                        set max_update_dt = coalesce(
                            ( select max(update_dt) from de10.fzlv_stg_accounts ),
                            ( select max(create_dt) from de10.fzlv_stg_accounts ),
                            ( select max_update_dt from de10.fzlv_meta_max_update_dt where schema_name = 'de10' and table_name = 'fzlv_accounts' ) )
                        where schema_name = 'de10' and table_name = 'fzlv_accounts' """ )
                        
# Обновление метаданных fzlv_cards

cursor_dwh.execute( """ update de10.fzlv_meta_max_update_dt
                        set max_update_dt = coalesce(
                            ( select max(update_dt) from de10.fzlv_stg_cards ),
                            ( select max(create_dt) from de10.fzlv_stg_cards ),
                            ( select max_update_dt from de10.fzlv_meta_max_update_dt where schema_name = 'de10' and table_name = 'fzlv_cards' ) )
                        where schema_name = 'de10' and table_name = 'fzlv_cards' """ )

# Обновление метаданных fzlv_clients

cursor_dwh.execute( """ update de10.fzlv_meta_max_update_dt
                        set max_update_dt = coalesce(
                            ( select max(update_dt) from de10.fzlv_stg_clients ),
                            ( select max(create_dt) from de10.fzlv_stg_clients ),
                            ( select max_update_dt from de10.fzlv_meta_max_update_dt where schema_name = 'de10' and table_name = 'fzlv_clients' ) )
                        where schema_name = 'de10' and table_name = 'fzlv_clients' """ )


####################################################
## Загрузка фактов
####################################################

# dwh_fact_passport_blacklist

cursor_dwh.execute( f""" insert into de10.fzlv_dwh_fact_passport_blacklist (
                            passport_num,
                            entry_dt
                        )
                        select
                            passport_num,
                            entry_dt
                        from de10.fzlv_stg_blacklist
                        where entry_dt = to_date('{report_dt}', 'DDMMYYYY') """ )

# dwh_fact_transactions

cursor_dwh.execute( """ insert into de10.fzlv_dwh_fact_transactions (
                            trans_id,
                            trans_date,
                            card_num,
                            oper_type,
                            amt,
                            oper_result,
                            terminal
                        )
                        select
                            stg.transaction_id,
                            to_timestamp( stg.transaction_date, 'YYYY-MM-DD HH24:MI:SS' ),
                            stg.card_num,
                            stg.oper_type,   
                            cast(replace(stg.amount, ',', '.') as numeric(8,2)),
                            stg.oper_result,
                            stg.terminal
                        from de10.fzlv_stg_transactions stg
                        left join de10.fzlv_dwh_fact_transactions tgt
                        on stg.transaction_id = tgt.trans_id
                        where tgt.trans_id is null """)



####################################################
## Построение отчетов
####################################################

# Отчет №1. Совершение операции при просроченном или заблокированном паспорте

cursor_dwh.execute( f""" insert into de10.fzlv_rep_fraud (
                            event_dt,
                            passport,
                            fio,
                            phone,
                            event_type,
                            report_dt )
                        select
                            t.trans_date,
                            cl.passport_num,
                            concat( cl.last_name, ' ', cl.first_name, ' ', cl.patronymic ),
                            cl.phone,
                            1,
                            to_date('{report_dt}', 'DDMMYYYY')
                        from de10.fzlv_dwh_fact_transactions t
                        inner join de10.fzlv_dwh_dim_cards_hist c on c.card_num = t.card_num
                            and c.deleted_flg = 'N'
                            and c.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                        inner join de10.fzlv_dwh_dim_accounts_hist a on a.account_num = c.account_num
                            and a.deleted_flg = 'N'
                            and a.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                        inner join de10.fzlv_dwh_dim_clients_hist cl on cl.client_id = a.client
                            and cl.deleted_flg = 'N'
                            and cl.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                        where
                            t.trans_date::date = to_date('{report_dt}', 'DDMMYYYY')
                            and (
                                cl.passport_valid_to < to_date('{report_dt}', 'DDMMYYYY')
                                or
                                cl.passport_num in ( select passport_num
                                                        from de10.fzlv_dwh_fact_passport_blacklist
                                                        where entry_dt <= to_date('{report_dt}', 'DDMMYYYY') ) 
                            ) """ )

# Отчет №2. Совершение операции при недействующем договоре 

cursor_dwh.execute( f""" insert into de10.fzlv_rep_fraud (
                            event_dt,
                            passport,
                            fio,
                            phone,
                            event_type,
                            report_dt )
                        select
                            t.trans_date,
                            cl.passport_num,
                            concat( cl.last_name, ' ', cl.first_name, ' ', cl.patronymic ),
                            cl.phone,
                            2,
                            to_date('{report_dt}', 'DDMMYYYY')
                        from de10.fzlv_dwh_fact_transactions t
                        inner join de10.fzlv_dwh_dim_cards_hist c on c.card_num = t.card_num
                            and c.deleted_flg = 'N'
                            and c.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                        inner join de10.fzlv_dwh_dim_accounts_hist a on a.account_num = c.account_num
                            and a.deleted_flg = 'N'
                            and a.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                        inner join de10.fzlv_dwh_dim_clients_hist cl on cl.client_id = a.client
                            and cl.deleted_flg = 'N'
                            and cl.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                        where
                            t.trans_date::date = to_date('{report_dt}', 'DDMMYYYY')
                            and t.trans_date > a.valid_to """ )
                                
# Отчет №2. Совершение операций в разных городах в течение одного часа

cursor_dwh.execute( f""" insert into de10.fzlv_rep_fraud (
                            event_dt,
                            passport,
                            fio,
                            phone,
                            event_type,
                            report_dt )
                        select
                            next_trans_date event_dt,
                            passport_num,
                            fio,
                            phone,
                            3,
                            to_date('{report_dt}', 'DDMMYYYY')
                        from (
                            select
                                t.trans_date,
                                lead( t.trans_date ) over ( partition by t.card_num order by t.trans_date ) next_trans_date,
                                cl.passport_num,
                                concat( cl.last_name, ' ', cl.first_name, ' ', cl.patronymic ) fio,
                                cl.phone,
                                tm.terminal_city,
                                lead( tm.terminal_city ) over ( partition by t.card_num order by t.trans_date ) next_terminal_city
                            from
                                de10.fzlv_dwh_fact_transactions t
                                inner join de10.fzlv_dwh_dim_cards_hist c on c.card_num = t.card_num
                                    and c.deleted_flg = 'N'
                                    and c.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                                inner join de10.fzlv_dwh_dim_accounts_hist a on a.account_num = c.account_num
                                    and a.deleted_flg = 'N'
                                    and a.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                                inner join de10.fzlv_dwh_dim_clients_hist cl on cl.client_id = a.client
                                    and cl.deleted_flg = 'N'
                                    and cl.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                                inner join de10.fzlv_dwh_dim_terminals_hist tm on tm.terminal_id = t.terminal
                                    and tm.deleted_flg = 'N'
                                    and tm.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                            where t.trans_date::date = to_date('{report_dt}', 'DDMMYYYY')
                        ) x
                        where
                            next_trans_date is not null
                            and extract( hour from next_trans_date - trans_date ) = 0
                            and terminal_city <> next_terminal_city """ )

conn_dwh.commit()

# Закрываем соединение
cursor_src.close()
cursor_dwh.close()

conn_src.close()
conn_dwh.close()

## Переносим обработанные файлы в архив
os.rename('/home/de10/fzlv/project/terminals_' + report_dt + '.xlsx', '/home/de10/fzlv/project/archive/terminals_' + report_dt + '.xlsx.backup')
os.rename('/home/de10/fzlv/project/passport_blacklist_' + report_dt + '.xlsx', '/home/de10/fzlv/project/archive/passport_blacklist_' + report_dt + '.xlsx.backup')
os.rename('/home/de10/fzlv/project/transactions_' + report_dt + '.txt', '/home/de10/fzlv/project/archive/transactions_' + report_dt + '.txt.backup')