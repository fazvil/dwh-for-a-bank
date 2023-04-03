create table de10.fzlv_stg_terminals (
    terminal_id      varchar(10),
    terminal_type    varchar(10),
    terminal_city    varchar(20),
    terminal_address varchar(200)
);

create table de10.fzlv_stg_cards (
    card_num  char(20),
    account   char(20),
    create_dt timestamp(0),
    update_dt timestamp(0)
);

create table de10.fzlv_stg_accounts (
    account   char(20),
    valid_to  date,
    client    varchar(10),
    create_dt timestamp(0),
    update_dt timestamp(0)
);

create table de10.fzlv_stg_clients (
    client_id         varchar(10),
    last_name         varchar(20),
    first_name        varchar(20),
    patronymic        varchar(20),
    date_of_birth     date,
    passport_num      varchar(15),
    passport_valid_to date,
    phone             char(16),
    create_dt         timestamp(0),
    update_dt         timestamp(0)
);

create table de10.fzlv_stg_blacklist (
    entry_dt     date,
    passport_num varchar(15)
);

create table de10.fzlv_stg_transactions (
    transaction_id   varchar(11),
    transaction_date varchar(20),
    amount           varchar(8),
    card_num         varchar(20),
    oper_type        varchar(10),
    oper_result      varchar(10),
    terminal         varchar(10)
);

create table de10.fzlv_dwh_dim_terminals_hist (
    terminal_id      varchar(10),
    terminal_type    varchar(10),
    terminal_city    varchar(20),
    terminal_address varchar(200),
    effective_from   timestamp(0),
    effective_to     timestamp(0),
    deleted_flg      char(1)
);

create table de10.fzlv_dwh_dim_cards_hist (
    card_num         char(20),
    account_num      char(20),
    effective_from   timestamp(0),
    effective_to     timestamp(0),
    deleted_flg      char(1)
);

create table de10.fzlv_dwh_dim_accounts_hist (
    account_num      char(20),
    valid_to         date,
    client           varchar(10),
    effective_from   timestamp(0),
    effective_to     timestamp(0),
    deleted_flg      char(1)
);

create table de10.fzlv_dwh_dim_clients_hist (
    client_id         varchar(10),
    last_name         varchar(20),
    first_name        varchar(20),
    patronymic        varchar(20),
    date_of_birth     date,
    passport_num      varchar(15),
    passport_valid_to date,
    phone             char(16),
    effective_from    timestamp(0),
    effective_to      timestamp(0),
    deleted_flg       char(1)
);

create table de10.fzlv_dwh_fact_passport_blacklist (
    passport_num varchar(15),
    entry_dt     date
);

create table de10.fzlv_dwh_fact_transactions (
    trans_id    char(11),
    trans_date  timestamp(0),
    card_num    varchar(20),
    oper_type   varchar(10),
    amt         numeric(8,2),
    oper_result varchar(10),
    terminal    varchar(10)
);

create table fzlv_rep_fraud (
    event_dt   timestamp(0),
    passport   varchar(15),
    fio        varchar (60),
    phone      char (16),
    event_type smallint,
    report_dt  date
);

create table de10.fzlv_meta_max_update_dt (
    schema_name varchar(30),
    table_name varchar(30),
    max_update_dt timestamp(0)
);

insert into de10.fzlv_meta_max_update_dt ( schema_name, table_name, max_update_dt )
values( 'de10','fzlv_accounts', to_timestamp('1900-01-01','YYYY-MM-DD') );
insert into de10.fzlv_meta_max_update_dt ( schema_name, table_name, max_update_dt )
values( 'de10','fzlv_cards', to_timestamp('1900-01-01','YYYY-MM-DD') );
insert into de10.fzlv_meta_max_update_dt ( schema_name, table_name, max_update_dt )
values( 'de10','fzlv_clients', to_timestamp('1900-01-01','YYYY-MM-DD') );

create table de10.fzlv_stg_del_accounts (
	account_num char(20)
);

create table de10.fzlv_stg_del_cards (
	card_num char(20)
);

create table de10.fzlv_stg_del_clients (
	client_id varchar(10)
);

create table de10.fzlv_stg_del_terminals (
	terminal_id varchar(10)
);