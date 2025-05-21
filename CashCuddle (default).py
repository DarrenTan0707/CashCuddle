import argparse
import sys
import pandas as pd
import psycopg2
from decimal import Decimal, ROUND_HALF_UP

DB_CONFIG = {
    "dbname":   "",
    "user":     "",
    "password": "",
    "host":     "",
    "port":     ""
}

def add_to_initial_balance(amount: float) -> Decimal:
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE initial_balance
                   SET balance = balance + %s
                 WHERE id = 1
                RETURNING balance
                """,
                (amount,)
            )
            new_balance = Decimal(cur.fetchone()[0]).quantize(Decimal("0.00"), ROUND_HALF_UP)
        conn.commit()
    return new_balance

def get_current_balance() -> Decimal:
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT balance FROM initial_balance WHERE id = 1")
            balance = Decimal(cur.fetchone()[0]).quantize(Decimal("0.00"), ROUND_HALF_UP)
    return balance

def determine_tier(item_type: str, cost: Decimal) -> str:
    if item_type == 'meal':
        if cost <= 6:
            return 'saving'
        elif cost < 15:
            return 'balance'
        else:
            return 'luxury'
    elif item_type == 'beverage':
        if cost <= 2:
            return 'saving'
        elif cost < 7:
            return 'balance'
        else:
            return 'luxury'
    else:
        raise ValueError(f"Unknown item type: {item_type}")

def create_tables():
    ddl = """
    CREATE TABLE IF NOT EXISTS initial_balance (
      id      INT PRIMARY KEY DEFAULT 1,
      balance DECIMAL(10,2) NOT NULL
    );
    INSERT INTO initial_balance(id,balance)
      VALUES (1,n) 
    ON CONFLICT(id) DO NOTHING;

    CREATE TABLE IF NOT EXISTS expenses (
      id                SERIAL PRIMARY KEY,
      date              DATE        NOT NULL,
      item              TEXT        NOT NULL,
      category              TEXT CHECK(type IN ('meal','beverage')),
      quantity          INT         NOT NULL,
      tier              TEXT        NOT NULL,
      cost              DECIMAL(10,2) NOT NULL,
      remaining_balance DECIMAL(10,2),
      processed         BOOLEAN     DEFAULT TRUE
    );
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()

def read_excel(path: str) -> pd.DataFrame:
    df = pd.read_excel(
        path,
        dtype={
            'Date': str,
            'Item': str,
            'Category': str,
            'Quantity': int,
            'Cost': float
        }
    )
    df['Date'] = pd.to_datetime(df['Date'], dayfirst=True, errors='raise')
    required = {'Date', 'Item', 'Category', 'Quantity', 'Cost'}
    if missing := required - set(df.columns):
        raise ValueError(f"Missing columns: {missing}")
    df['Item'] = df['Item'].str.title().str.strip()
    df['Category'] = df['Category'].str.lower().str.strip()
    df['Cost'] = df['Cost'].apply(
        lambda v: Decimal(str(v)).quantize(Decimal("0.00"), ROUND_HALF_UP))
    return df

def sync_expenses(df: pd.DataFrame):
    running_balance = get_current_balance()
    df_keys = {
        (r.Date.date(), r.Item, r.Category, r.Quantity, r.Cost)
        for r in df.itertuples()
    }

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, date, item, category, quantity, cost
                  FROM expenses
                ORDER BY date, id
                """
            )
            existing = cur.fetchall()

            to_delete = [row[0] for row in existing
                         if (row[1], row[2], row[3], row[4], row[5]) not in df_keys]
            if to_delete:
                cur.execute(
                    "DELETE FROM expenses WHERE id = ANY(%s)",
                    (to_delete,)
                )
                print(f"🗑 Deleted {len(to_delete)} transaction(s)")

            existing_keys = {
                (r[1], r[2], r[3], r[4], r[5])
                for r in existing if r[0] not in to_delete
            }

            inserts = []
            for row in df.sort_values('Date').itertuples():
                
                key = (row.Date.date(), row.Item, row.Category, row.Quantity, row.Cost)
                amount_spent = row.Cost * row.Quantity
                remaining = running_balance.quantize(Decimal("0.00"), ROUND_HALF_UP)

                if key in existing_keys:
                    cur.execute("""
                        UPDATE expenses
                           SET amount_spent = %s,
                           remaining_balance = %s
                         WHERE date = %s
                           AND item = %s
                           AND category = %s
                           AND quantity = %s
                           AND cost = %s
                    """, (
                        amount_spent,
                        remaining,
                        row.Date.date(),
                        row.Item,
                        row.Category,
                        row.Quantity,
                        row.Cost
                    ))
                else:
                    running_balance -= row.Cost * row.Quantity
                    remaining = running_balance.quantize(Decimal("0.00"), ROUND_HALF_UP)
                    inserts.append(
                        (row.Date.date(), row.Item, row.Category,
                         row.Quantity, determine_tier(row.Category, float(row.Cost)),
                         row.Cost, amount_spent, remaining)
                    )

            if inserts:
                cur.executemany(
                    """
                    INSERT INTO expenses
                      (date, item, category, quantity, tier, cost, amount_spent, remaining_balance)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    inserts
                )
                print(f"✅ Inserted {len(inserts)} new expense(s)")
            else:
                print("🔄 No new expenses")

            cur.execute(
                "UPDATE initial_balance SET balance = %s WHERE id = 1",
                (running_balance.quantize(Decimal("0.00"), ROUND_HALF_UP),)
            )
            conn.commit()

            print(f"Remaining balance : ${running_balance:.2f}")

def main():
    parser = argparse.ArgumentParser(description="CashCuddle")
    parser.add_argument(
        "-d", "--deposit", type=float,
        help="Deposit (+) or withdraw (−); 0 to skip"
    )
    parser.add_argument(
        "-f", "--file", default="CashCuddle.xlsx",
        help="Excel file path"
    )
    args, _ = parser.parse_known_args()

    interactive = not sys.argv[1:] or 'idlelib' in sys.modules
    create_tables()

    net_adj = 0.0
    current_balance = get_current_balance()
    
    if interactive:
        dep = input("Enter deposit amount (or press Enter to skip): ").strip()
        if dep and float(dep) != 0:
            net_adj = float(dep)
            current_balance = add_to_initial_balance(net_adj)
            print(f"💰 Deposited ${net_adj:.2f}")
        elif not dep:
            wd = input("Enter withdraw amount (or press Enter to skip): ").strip()
            if wd and float(wd) != 0:
                net_adj = -float(wd)
                current_balance = add_to_initial_balance(net_adj)
                print(f"💸 Withdrawn ${abs(net_adj):.2f}")
    else:
        if args.deposit not in (None, 0.0):
            net_adj = args.deposit
            current_balance = add_to_initial_balance(net_adj)
            if net_adj > 0:
                print(f"💰 Deposited ${net_adj:.2f}")
            else:
                print(f"💸 Withdrawn ${abs(net_adj):.2f}")

    print(f"Balance : ${current_balance:.2f}")

    path = "CashCuddle.xlsx" if interactive else args.file
    try:
        df = read_excel(path)
        sync_expenses(df)
    except FileNotFoundError:
        if net_adj == 0:
            print(f"⚠️  File not found: {path}")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
