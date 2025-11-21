# zenoh-clj (wip)

> very much a wip!

Clojure wrapper for [Eclipse Zenoh](https://zenoh.io).

Built on top of [zenoh-java](https://github.com/eclipse-zenoh/zenoh-java).

[![Clojars Project](https://img.shields.io/clojars/v/dev.awshaw/zenoh-clj.svg)](https://clojars.org/dev.awshaw/zenoh-clj)

## Usage WIP

In `bin/` there's an example script for installing Zenoh. I've had trouble installing via homebrew, and found building was the most straightforward (reference the docs for installing/managing Zenoh plugins [plugin docs](https://zenoh.io/docs/manual/plugin-storage-manager/)). Note, there is also a docker image for running Zenoh. 

TODO:
- [x] Query/Queryable API
- [ ] Proper encoding enum mapping
- [ ] Config validation and better JSON serialization
- [ ] Options support
- [ ] Error handling

## Useful docs

- [Zenoh Documentation](https://zenoh.io/docs)
- [zenoh-java API Docs](https://eclipse-zenoh.github.io/zenoh-java/)
- [Zenoh reference implementation](https://github.com/eclipse-zenoh/zenoh)

## License

Same as Zenoh: Apache 2.0 / EPL 2.0
