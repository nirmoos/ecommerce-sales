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
