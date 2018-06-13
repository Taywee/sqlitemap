# sqlitemap

An attempt at replicating much of the HashMap interface and trait
implementations but using sqlite as a backend instead.  Will probably
necessitate a user-specified encoder and decoder pair for most types, and the
user's type will have to implement the encoder and decoder traits.  Might
piggy-back on serde's or some other serialize traits to ensure that this works
easily.  Something to ensure that any selected key and value structure can be
converted unambiguously to and from a byte array.
