# MongoFS
Access Mongo documents in a FUSE filesystem

## Background
Sometimes a human needs to be view or edit a [Mongo](https://www.mongodb.org/) document. And when humans are involved, a good UI helps a lot!

I have been using [Robomongo](http://robomongo.org/) for that. It is a fine UI, but it doesn't compare with being able to use my favorite `$EDITOR`.

When I realized I was copy-pasting between robomongo and my text editor all the time, I decided It was time to fix that!

## Features

MongoFS exposes documents as `/{database}/{collection}/{document_id}.json`

Supported Operations:
|          | Create | Delete | Rename | Read | Write |
|----------|--------|--------|--------|------|-------|
|Database  | ✓      | ✓      | ✓      |      |       |
|Collection| ✓      | ✓      | ☐      |      |       |
|Document  | ☐      | ☐      | ☐      | ✓    | ✓    |
