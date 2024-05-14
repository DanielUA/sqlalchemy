from sqlalchemy import desc, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, and_, or_, not_, func
from sqlalchemy.orm import relationship

engine = create_engine("sqlite:///:memory:", echo=True)
DBSession = sessionmaker(bind=engine)
session = DBSession()

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    fullname = Column(String(120))

class Address(Base):
    __tablename__ = "addresses"
    id = Column(Integer, primary_key=True)
    email = Column(String(50), nullable=False)
    user_id = Column(Integer, ForeignKey('users.id'))
    user = relationship("User", back_populates="addresses")

User.addresses = relationship("Address", order_by=Address.id, back_populates="user")

Base.metadata.create_all(engine)

if __name__ == "__main__":
    # session.query(User).delete()
    # session.query(Address).delete()
    # session.commit()

    n_user = User(fullname="Jack Jones")
    session.add(n_user)
    n_address = Address(email="jackie@hotmail.com", user=n_user)
    session.add(n_address)
    n_address = Address(email="jackichan@hotmail.com", user=n_user)
    session.add(n_address)

    n_user = User(fullname="Jank Jomet")
    session.add(n_user)
    n_address = Address(email="jackie@hotmail.com", user=n_user)
    session.add(n_address)

    
    n_user = User(fullname="Vasya Pupkin")
    session.add(n_user)
    n_address = Address(email="pupik@gmail.com", user=n_user)
    session.add(n_address)
    n_address = Address(email="pepik@gmail.com", user=n_user)
    session.add(n_address)
    n_address = Address(email="popok@gmail.com", user=n_user)
    session.add(n_address)
    n_address = Address(email="repikt@gmail.com", user=n_user)
    session.add(n_address)

    n_user = User(fullname="Adam Smith")
    session.add(n_user)
    n_address = Address(email="adam@gmail.com", user=n_user)
    session.add(n_address)

    session.commit()

    # statement = select(User.id, User.fullname)
    # for row in session.execute(statement):
    #     print(row)

    # statement = select(Address.id, Address.email, User.fullname).join(User)
    # for row in session.execute(statement):
    #     print(row)
    
    #OR
    # statement = select(User).where(or_(User.fullname.like('%a%'), User.id==1))

    # r=session.execute(statement)
    # for row in r:
    #     print(row[0].fullname)

    statement = select(User).order_by(desc(User.fullname))
    columns = ["id", "fullname"]   
    result = [dict(zip(columns, (row.User.id, row.User.fullname))) for row in session.execute(statement)]
    print(result)    

    # result = [dict(zip(columns, (row.id, row.fullname))) for row in session.execute(statement).scalars()]
    # print(result)
    
    statement = (select(User.fullname, func.count(Address.id))
                 .join(Address).group_by(User.fullname))
    

    r = session.execute(statement=statement).all()
    print(r )

    # statement = select(User).where(User.fullname.like('%J%'))
    # r = session.execute(statement=statement).scalars()
    # for row in r:
    #     print(row.fullname)
 
    # statement = select(User).where(and_(User.fullname.like('%a%'), User.id == 3))
    # r = session.execute(statement=statement).scalars()
    # for row in r:
    #     print(row.fullname)
    
    session.close()
