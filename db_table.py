from sqlalchemy import BOOLEAN, TIMESTAMP, Column, ForeignKey, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class UserDB(Base):
    """Database table to store unique users"""
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user = Column(String, index=True)

    def __repr__(self):
        return f"User(id={self.id!r}, path={self.user!r})"


class URLsDB(Base):
    """Database table to store unique urls"""
    __tablename__ = 'urls'

    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(String, index=True)

    def __repr__(self):
        return f"User(id={self.id!r}, path={self.url!r})"


class CommentsDB(Base):
    __tablename__ = 'comments'

    id = Column(Integer, primary_key=True, autoincrement=True)
    path = Column(String, index=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    url_id = Column(Integer, ForeignKey('urls.id'))
    comment = Column(String)
    date = Column(TIMESTAMP)
    last = Column(BOOLEAN)

    def __repr__(self):
        return f"Comment(id={self.id!r}, path={self.path!r}," \
               f" user_id={self.user_id!r}, url_id={self.url_id!r}," \
               f" comment={self.comment!r}, date={self.date!r}, " \
               f" last={self.last!r})"
