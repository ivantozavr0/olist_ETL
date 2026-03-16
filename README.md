# Это проект платформы обработки данных (Data Platform) для e-commerce аналитики.

В данном поекте используется датасет "Brazilian E-Commerce Public Dataset by Olist", распространяемый под лицензией CC BY-NC-SA 4.0.

Пайплайн автоматически:

- загружает сырые данные с kagglehub

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




