from sqlalchemy import Table, Column, Integer, String, ForeignKey, MetaData, create_engine, join
from sqlalchemy.sql import select


metadata = MetaData()

engine = create_engine("sqlite:///:memory:", echo=True)

users = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("fullname", String),
)

addresses = Table(
    "addresses",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("email", String, nullable=False),
    Column("user_id", Integer, ForeignKey("users.id")),
    )

metadata.create_all(engine)


if __name__=="__main__":
    with engine.connect() as conn:
        ins_user = users.insert().values(fullname='Jack Jones')
        result = conn.execute(ins_user)
        jones_id = result.lastrowid
        print(jones_id)
        
        ins_user = users.insert().values(fullname='Vasya Pupkin')
        result = conn.execute(ins_user)
        pupkin_id = result.lastrowid
        print(pupkin_id)

        result = conn.execute(select(users))
        for row in result:
            print(row)

        ins_adrs = addresses.insert().values(email='ja_ja@gmail.com', user_id=jones_id)
        conn.execute(ins_adrs)
        
        ins_adrs = addresses.insert().values(email='vas_pup@gmail.com', user_id=pupkin_id)
        conn.execute(ins_adrs)
            
        
        result = conn.execute(select(addresses))
        for row in result:
            print(row)   
            
        sql_select = select(addresses.c.id, addresses.c.email, users.c.fullname).join(users)
        result = conn.execute(sql_select)
        for row in result:
            print(row)