#Это проект платформы обработки данных (Data Platform) для e-commerce аналитики.

Пайплайн автоматически:

- загружает сырые данные

- очищает и трансформирует их

- строит star schema

- формирует аналитические витрины

- визуализирует метрики в Metabase

### Используется медальонная архитектура (Bronze -> Silver -> DWH (ClickHouse) -> Витрины (мат. представления в ClickHouse))


Raw Data
   │
   ▼
Bronze Layer (Raw ingestion)
   │
   ▼
Silver Layer (Cleaned data)
   │
   ▼
Star Schema (ClickHouse DWH)
   │
   ▼
Data Marts (Analytical Views)
   │
   ▼
BI Dashboard (Metabase)

