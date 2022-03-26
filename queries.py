TABLE_CHECK_AND_ROW_COUNT_QUERY = (
    """
        SELECT 
            TABLE_NAME, 
            TABLE_ROWS 
        FROM 
            information_schema.tables 
        WHERE 
            table_schema='ecommerce_sales' 
        AND 
            table_name='sales_data'
    """
)
CREATE_TABLE_QUERY = (
    """
        CREATE TABLE sales_data (
            id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
            date DATETIME,
            invoice_no VARCHAR(15),
            stock_code VARCHAR(15),
            description TINYTEXT,
            quantity INT,
            unit_price DECIMAL(10, 2),
            customer_id INT,
            country VARCHAR(30)
        )
    """
)
TRUNCATE_TABLE_QUERY = (
    """
        TRUNCATE TABLE sales_data
    """
)
DATA_LOADING_QUERY = (
    """
        INSERT INTO sales_data (%s) VALUES %s
    """
)
CUSTOMER_PURCHASED_HIGHEST_QUANTITY_QUERY = (
    """
        SELECT 
            casted_date date, 
            customer_id, 
            total_quantity quantity 
        FROM (
            SELECT 
                casted_date, 
                customer_id, 
                total_quantity, 
                RANK() OVER (PARTITION BY casted_date ORDER BY total_quantity DESC) quantity_rank 
            FROM (
                SELECT 
                    DATE(date) casted_date, 
                    customer_id, 
                    SUM(quantity) total_quantity 
                FROM 
                    sales_data 
                WHERE 
                    customer_id IS NOT NULL 
                GROUP BY 
                    casted_date, 
                    customer_id
            )  a
        ) b 
        WHERE 
            quantity_rank = 1 
        ORDER BY 
            casted_date DESC
    """
)
CUSTOMER_PURCHASED_HIGHEST_QUANTITY_QUERY_ANOTHER_QUERY = (
    """
        WITH temp AS (
            SELECT 
                DATE(date) casted_date, 
                customer_id, 
                SUM(quantity) total_quantity 
            FROM 
                sales_data 
            WHERE 
                customer_id IS NOT NULL 
            GROUP BY 
                casted_date, customer_id
        )
        SELECT 
            casted_date, 
            customer_id, 
            total_quantity 
        FROM 
            temp 
        INNER JOIN (
            SELECT 
                casted_date cd, 
                MAX(total_quantity) max_q 
            FROM 
                temp 
            GROUP BY 
                casted_date
        ) AS a 
        ON 
            temp.casted_date=a.cd 
        AND 
            temp.total_quantity = a.max_q 
        ORDER BY 
            casted_date DESC
    """
)
TOP_10_CUSTOMERS_WITH_SALES_AMOUNT = (
    """
        SELECT 
            customer_id, 
            ROUND(SUM(quantity * unit_price), 2) sales_amount 
        FROM 
            sales_data 
        WHERE
            customer_id IS NOT NULL
        GROUP BY 
            customer_id 
        ORDER BY 
            sales_amount DESC 
        LIMIT 
            10
    """
)
TOP_10_PRODUCTS_PURCHASES = (
    """
        SELECT 
            stock_code, 
            JSON_EXTRACT(JSON_ARRAYAGG(description), '$[0]') stock_name, 
            SUM(quantity) total_sale 
        FROM 
            sales_data 
        WHERE
            stock_code IS NOT NULL
        GROUP BY 
            stock_code 
        ORDER BY 
            total_sale DESC 
        LIMIT 10
    """
)
