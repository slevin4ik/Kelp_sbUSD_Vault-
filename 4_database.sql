-- Специально схему DeFi Vaults делаем с 4 таблицами
CREATE SCHEMA IF NOT EXISTS defi_vaults;

SET search_path TO defi_vaults, public;

-- Таблица блокчейнов
CREATE TABLE chains (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,  -- 'ethereum', 'optimism', 'polygon' ….
    chain_id INTEGER NOT NULL UNIQUE,  
    rpc_url TEXT,                      --  вариант еще сюда закинуть в виде json несколько rpc
    created_at TIMESTAMP DEFAULT NOW() 
);

-- реестр волтов
CREATE TABLE vaults (
    id SERIAL PRIMARY KEY,
    chain_id INTEGER REFERENCES chains(id),
    address VARCHAR(42) NOT NULL,      
    name VARCHAR(100) NOT NULL,        -- 'Kelp sbUSD Vault'
    symbol VARCHAR(20),                -- 'sbUSD'
    asset_address VARCHAR(42)         --  База DAI/USDT/USDC
);

-- таблица с основными метриками в динамике
CREATE TABLE vaults_performance (
    id BIGSERIAL PRIMARY KEY,
    vault_id INTEGER REFERENCES vaults(id),
    block_number BIGINT,
    utc_datetime TIMESTAMP NOT NULL,    
    tvl_usd NUMERIC(30,2) NOT NULL,   
    share_price NUMERIC(30,8) NOT NULL,
    total_supply NUMERIC(30,8) NOT NULL,
    asset_decimals SMALLINT NOT null
);
    
    -- Производные метрики во view потом нужны
-- apr_7d NUMERIC(10,4),             -- APR за 7 дней
-- apr_7d NUMERIC(10,4),             -- APR за 7 дней
-- apr_30d NUMERIC(10,4),            -- APR за 30 дней
    
-- логи изменений в vaults_performance
CREATE TABLE vaults_changes (
    id BIGSERIAL PRIMARY KEY,
    vaults_performance_id INTEGER REFERENCES vaults_performance(id),
    vaults_metric_name VARCHAR(50) NOT NULL,  -- 'tvl_usd', 'share_price'
    old_value NUMERIC(30,8),
    new_value NUMERIC(30,8),
    changed_at TIMESTAMP DEFAULT NOW()
);



ALTER TABLE defi_vaults.vaults_performance 
ADD CONSTRAINT unique_vault_time UNIQUE (vault_id, block_number);  -- ограничение блок и волт


ALTER TABLE defi_vaults.vaults 
ADD CONSTRAINT unique_chain_address UNIQUE (chain_id, address);

