The "snappy" library is required for Kafka. Make sure to install this and put the location
of the library on `$LIBRARY_PATH` before building:

Mac:
```
  brew install snappy
  export LIBRARY_PATH="/usr/local/lib"
```

Now build Collector:
```
  cargo build --release
```
