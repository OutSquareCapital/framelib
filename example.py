import dataframely as dy

import framepaths as fp


class BaseSchema(fp.CSVSchema):
    __directory__ = "bookshop"


class Users(BaseSchema):
    name = dy.String(nullable=False, min_length=3)
    age = dy.UInt8(nullable=False)
    email = dy.String(nullable=False)


class Articles(BaseSchema):
    title = dy.String(nullable=False, min_length=3)


class Books(Articles):
    chapters = dy.List(dy.String(nullable=False), nullable=False)
    metadata = dy.Struct(
        {
            "pages": dy.UInt16(nullable=False),
            "isbn": dy.String(nullable=False, min_length=10),
        },
        nullable=False,
    )


class VideoGames(Articles):
    platform = dy.String(nullable=False, min_length=2)
    release_date = dy.Date(nullable=False)
    rating = dy.Float32(nullable=True, min=0.0, max=10.0)


class BookShop(dy.Collection):
    users: dy.LazyFrame[Users]
    books: dy.LazyFrame[Books]
    video_games: dy.LazyFrame[VideoGames]
