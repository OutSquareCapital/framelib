# Dataframely plugin for path files management

This plugin makes it easy to manage structured data files (CSV, Parquet, etc.) using typed Python schemas, leveraging the dataframely library.

You can find more infos about dataframely here:

<https://github.com/Quantco/dataframely>

## Installation

```bash
uv add git+https://github.com/OutSquareCapital/framepaths.git
```

## Main Concepts

- **CSV, Parquet, etc.**: The base classes (`fp.CSV`, `fp.Parquet`, etc.) represent typed data files. They are used as a foundation for defining data schemas.
- **Schema**: A schema is a class inheriting from a file format (e.g., `fp.CSV`) and describes columns and their types using dataframely attributes (`dy.String`, `dy.UInt8`, etc.).
- **Collection**: A collection groups several schemas (tables) into a typed structure, making grouped access and manipulation easy.
- **`__directory__`**: Special attribute to indicate the root folder where files associated with the schema are stored.

## Usage

Define your data schemas by inheriting from the base classes provided by framepaths, then group them into a collection. Each schema corresponds to a file (CSV, Parquet, etc.) in the specified directory.

### Example

```python
import framepaths as fp
import dataframely as dy

class BaseSchema(fp.CSV):
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
```

### Explanations

- `BaseSchema` sets the root directory (`bookshop`) for all CSV files.
- `Users`, `Articles`, `Books`, `VideoGames` are typed schemas for each table/file.
- `Books` and `VideoGames` inherit from `Articles` to factor out common columns.
- `BookShop` groups the different tables into a typed collection, each attribute being a `dy.LazyFrame` of the corresponding schema.

### File Organization

CSV files will automatically be read from/written to the `bookshop/` directory, with filenames matching the class names (e.g., `users.csv`, `books.csv`, etc.).
