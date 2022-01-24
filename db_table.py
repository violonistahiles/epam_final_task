from sqlalchemy import BOOLEAN, Column, Float, ForeignKey, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class UserDB(Base):
    """Database table to store unique users"""
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user = Column(String, index=True)

    def __repr__(self):
        return f"User(id={self.id!r}, path={self.user!r})"

    def get_dict(self):
        return {'id': f'{self.id!r}', 'user': f'{self.user!r}'}


class URLsDB(Base):
    """Database table to store unique urls"""
    __tablename__ = 'urls'

    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(String, index=True)

    def __repr__(self):
        return f"User(id={self.id!r}, path={self.url!r})"

    def get_dict(self):
        return {'id': f'{self.id!r}', 'url': f'{self.url!r}'}


class CommentsDB(Base):
    """Database table to store unique comments"""
    __tablename__ = 'comments'

    id = Column(Integer, primary_key=True, autoincrement=True)
    path = Column(String, index=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    url_id = Column(Integer, ForeignKey('urls.id'))
    comment = Column(String)
    date = Column(Float)
    last = Column(BOOLEAN)

    def __repr__(self):
        return f"Comment(id={self.id!r}, path={self.path!r}," \
               f" user_id={self.user_id!r}, url_id={self.url_id!r}," \
               f" comment={self.comment!r}, date={self.date!r}, " \
               f" last={self.last!r})"

    def get_dict(self):
        return {'id': f'{self.id!r}', 'path': f'{self.path!r}',
                'user_id': f'{self.user_id!r}', 'url_id': f'{self.url_id!r}',
                'comment': f'{self.comment!r}', 'date': f'{self.date!r}',
                'last': f'{self.last!r}'}
