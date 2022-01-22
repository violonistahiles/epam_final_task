from sqlalchemy import BOOLEAN, TIMESTAMP, Column, ForeignKey, Integer, String
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class UserDB(Base):
    """Database table to store unique users"""
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user = Column(String, index=True)


class URLsDB(Base):
    """Database table to store unique urls"""
    __tablename__ = 'urls'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user = Column(String, index=True)


class CommentsDB(Base):
    __tablename__ = 'comments'

    id = Column(Integer, primary_key=True, autoincrement=True)
    path = Column(String, index=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    url_id = Column(Integer, ForeignKey('urls.id'))
    comment = Column(String)
    date = Column(TIMESTAMP)
    last = Column(BOOLEAN)

    homeworks = relationship('HomeworkResultTable', backref='student')

    def __repr__(self):
        return f"Comment(id={self.id!r}, path={self.path!r}," \
               f" user_id={self.user_id!r}, url_id={self.url_id!r})," \
               f" user_id={self.comment!r}, url_id={self.date!r}), " \
               f" user_id={self.last!r}"
