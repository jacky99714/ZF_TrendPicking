-- 藏鋒系統資料庫初始化腳本
-- 此腳本會在 PostgreSQL 容器首次啟動時自動執行

-- 股票基本資料
CREATE TABLE IF NOT EXISTS stock_info (
    stock_id VARCHAR(10) PRIMARY KEY,
    stock_name VARCHAR(50) NOT NULL,
    industry_category VARCHAR(50),
    stock_type VARCHAR(20),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- 每日股價資料
CREATE TABLE IF NOT EXISTS daily_price (
    id SERIAL PRIMARY KEY,
    stock_id VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open_price DECIMAL(12, 4),
    high_price DECIMAL(12, 4),
    low_price DECIMAL(12, 4),
    close_price DECIMAL(12, 4),
    volume BIGINT,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(stock_id, date)
);

-- 大盤指數
CREATE TABLE IF NOT EXISTS market_index (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    taiex DECIMAL(12, 4),
    created_at TIMESTAMP DEFAULT NOW()
);

-- 篩選結果
CREATE TABLE IF NOT EXISTS filter_result (
    id SERIAL PRIMARY KEY,
    filter_date DATE NOT NULL,
    filter_type VARCHAR(20) NOT NULL,
    stock_id VARCHAR(10) NOT NULL,
    stock_name VARCHAR(50) NOT NULL,
    industry_category VARCHAR(50),
    return_20d DECIMAL(8, 4),
    is_strong_list BOOLEAN,
    is_new_high_list BOOLEAN,
    today_price DECIMAL(12, 4),
    second_high_55d DECIMAL(12, 4),
    gap_ratio DECIMAL(8, 4),
    created_at TIMESTAMP DEFAULT NOW()
);

-- 索引優化
CREATE INDEX IF NOT EXISTS idx_daily_price_stock_date ON daily_price(stock_id, date DESC);
CREATE INDEX IF NOT EXISTS idx_daily_price_date ON daily_price(date);
CREATE INDEX IF NOT EXISTS idx_market_index_date ON market_index(date DESC);
CREATE INDEX IF NOT EXISTS idx_filter_result_date_type ON filter_result(filter_date, filter_type);

-- 輸出建立結果
SELECT 'Database initialized successfully' AS status;
