# Dom's `py2neo` Extras

A very small and simple collection of little shorthand methods I've developed and begun using to save time when developing with `py2neo`, the [Neo4J python drivere developed by Nigel Small](https://github.com/technige/py2neo).

I haven't had time to properly document and test these yet, and make no claims to it working 100% correctly. This code is open sourced mostly to force myself to make it more reusable across my projects, but hopefully it might save some time / provide a starting point for any work you're doing in the future. It should be python3 compatible, and is intended to work with `py2neo` version 3.

## Features

More detailed docs to follow tomorrow....

### `SingleRelated`

For when an entity has one, and only one relationship with a given label to a node with a given label.

### `RelatedToInChain`

For when you want to iterate over nodes chained together in a single path. Fluent interface for skipping and limiting.
