drop table if exists gtaa.ticker_price;
create table gtaa.ticker_price (
    ticker char(8) not null,
	trade_date date not null,
    open_price double,
    high_price double,
    low_price double,
    close_price double,
    adj_close_price double,
    volume int,
    trade_day int,
    rev_trade_day int,

    primary key(ticker, trade_date)
);
create unique index idx1 on gtaa.ticker_price(trade_date,ticker);

drop table if exists gtaa.ticker_group;
create table gtaa.ticker_group (
	group_id int not null,
    ticker char(8) not null,

    primary key(portfolio_id, ticker)
);

create or replace view gtaa.ticker_list as
select
	distinct tg.ticker,
	date(coalesce(max(tp.trade_date),'1999-01-01')) as last_trade_date
from gtaa.ticker_group tg
left join gtaa.ticker_price tp on tp.ticker=tg.ticker
group by ticker;

drop table if exists gtaa.portfolio;
create table gtaa.portfolio (
	portfolio_id int not null,
    portfolio_name varchar(16) not null,
    ticker_group_id int not null,
    trade_day int not null,
    ticker_count int not null,
    default_ticker char(8) not null,
    is_active boolean not null,

    primary key(portfolio_id)
);

create or replace view gtaa.ticker_sma as
SELECT ticker,
	trade_date,
	avg(adj_close_price) OVER (
 		partition by ticker ORDER BY trade_date
        rows 19 preceding
	) as sma_20d,
	avg(adj_close_price) OVER (
 		partition by ticker ORDER BY trade_date
        rows 49 preceding
	) as sma_50d,
	avg(adj_close_price) OVER (
 		partition by ticker ORDER BY trade_date
        rows 199 preceding
	) as sma_200d
FROM gtaa.ticker_price
ORDER BY ticker, trade_date DESC;


drop table if exists gtaa.portfolio_rule;
create table gtaa.portfolio_rule (
	rule_id int not null,
    portfolio_id int not null,
    rule_text varchar(512) not null,

    primary key(rule_id)
);
create unique index idx01 on gtaa.portfolio_rule(portfolio_id, rule_id);


create view gtaa.portfolio_close_price as
with t as (
	select p.portfolio_id, tg.ticker
	from gtaa.portfolio p
	join gtaa.ticker_group tg on tg.group_id=p.ticker_group_id
    union all
    select p.portfolio_id, p.default_ticker
    from gtaa.portfolio p
    where p.default_ticker <> ''
)
select t.portfolio_id, tp.trade_date, tp.ticker, tp.adj_close_price
from t
join gtaa.ticker_price tp on t.ticker=tp.ticker;


-- probably better in code
with vals as (
	SELECT ticker,
	trade_date,
    first_value(adj_close_price) over (
		partition by ticker ORDER BY trade_date
		rows 21 preceding
	) as val_21d,
    first_value(adj_close_price) over (
		partition by ticker ORDER BY trade_date
		rows 63 preceding
	) as val_63d,
    first_value(adj_close_price) over (
		partition by ticker ORDER BY trade_date
		rows 126 preceding
	) as val_126d,
    first_value(adj_close_price) over (
		partition by ticker ORDER BY trade_date
		rows 252 preceding
	) as val_252d
	FROM gtaa.ticker_price
)
select tp.ticker, tp.trade_date,
	tp.adj_close_price,
	(tp.adj_close_price - v.val_21d)/v.val_21d as ret_21d,
	(tp.adj_close_price - v.val_63d)/v.val_63d as ret_63d,
	(tp.adj_close_price - v.val_126d)/v.val_126d as ret_126d,
	(tp.adj_close_price - v.val_252d)/v.val_252d as ret_252d
from gtaa.ticker_price tp
join vals v on v.ticker=tp.ticker and v.trade_date=tp.trade_date
where tp.ticker='IWN'
order by ticker, trade_date desc

