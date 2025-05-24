from core.query_optimizer import QueryOptimizer
from core.db_storage import db_storage

def setup_test_data():
    """Create sample tables with test data."""
    # Create products table
    db_storage.create_table("products", ["id", "name", "base_price", "cost", "tax_rate"])
    products = [
        [1, "Laptop", "1200.00", "800.00", "0.10"],
        [2, "Mouse", "50.00", "20.00", "0.08"],
        [3, "Monitor", "400.00", "250.00", "0.10"],
        [4, "Keyboard", "80.00", "40.00", "0.08"]
    ]
    for product in products:
        db_storage.insert_row("products", product)

    # Create sales table
    db_storage.create_table("sales", [
        "id", "product_id", "quantity", "discount_percent", 
        "shipping_cost", "handling_fee"
    ])
    sales = [
        [1, 1, "2", "0.05", "25.00", "10.00"],  # Laptop x2
        [2, 2, "5", "0.10", "10.00", "5.00"],   # Mouse x5
        [3, 3, "1", "0.00", "30.00", "10.00"],  # Monitor x1
        [4, 4, "3", "0.15", "15.00", "5.00"],   # Keyboard x3
        [5, 1, "1", "0.20", "25.00", "10.00"]   # Laptop x1
    ]
    for sale in sales:
        db_storage.insert_row("sales", sale)

def test_inventory_valuation_query():
    """Test query with inventory valuation calculations."""
    optimizer = QueryOptimizer()
    
    query = """
    SELECT 
        p.id,
        p.name,
        -- Gross profit calculation
        (p.base_price - p.cost) as unit_profit,
        -- Profit margin percentage
        ((p.base_price - p.cost) / p.base_price * 100) as profit_margin,
        -- Tax amount
        (p.base_price * p.tax_rate) as unit_tax,
        -- Total value with tax
        (p.base_price * (1 + p.tax_rate)) as unit_total
    FROM data_products p
    WHERE ((p.base_price - p.cost) / p.base_price * 100) > 40
    ORDER BY ((p.base_price - p.cost) / p.base_price * 100) DESC;
    """
    
    print("\nInventory Valuation Query Test:")
    print("Original Query:")
    print(query)
    
    optimized_query = optimizer.optimize_query(query)
    print("\nOptimized Query:")
    print(optimized_query)
    
    print("\nQuery Results:")
    try:
        results = db_storage.execute_query(optimized_query)
        print("\n{:<4} {:<10} {:>12} {:>15} {:>12} {:>12}".format(
            "ID", "Product", "Unit Profit", "Profit Margin%", "Unit Tax", "Total"
        ))
        print("-" * 70)
        for row in results:
            print("{:<4} {:<10} {:>12.2f} {:>15.2f} {:>12.2f} {:>12.2f}".format(
                row['id'],
                row['name'],
                float(row['unit_profit']),
                float(row['profit_margin']),
                float(row['unit_tax']),
                float(row['unit_total'])
            ))
    except Exception as e:
        print(f"Error executing query: {e}")

def main():
    # Setup test data
    print("Setting up test data...")
    setup_test_data()
    
    # Run test query
    test_inventory_valuation_query()
    
    # Show all UDFs created
    print("\nAll Created UDFs:")
    for udf in db_storage.list_udfs():
        print(f"\nUDF: {udf['name']}")
        print(f"Expression: {udf['expression']}")
        print(f"Parameters: {', '.join(udf['parameters'])}")

if __name__ == "__main__":
    main() 