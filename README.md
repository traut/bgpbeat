# bgpbeat

Streaming live BGP updates from RIPE NCC RIS collectors into ES cluster.

## Usage

To run:

    $ ES_URL='https://<ES-ENDPOINT>:9243' \
        ES_AUTH_USERNAME=<USERNAME> \
        ES_AUTH_PASSWORD=<PASSWORD> \
        ES_INDEX=<DATA-STREAM-NAME> \
        lein run


To build uberjar, run `build.sh` or `lein uberjar`. With the jar execution command is

    $ ES_URL='https://<ES-ENDPOINT>:9243' \
        ES_AUTH_USERNAME=<USERNAME> \
        ES_AUTH_PASSWORD=<PASSWORD> \
        ES_INDEX=<DATA-STREAM-NAME> \
        java -jar bgpbeat-0.1.0-SNAPSHOT-standalone.jar [args]

### Configuration

Most of the configurable settings are available in the code. Good luck!

### References

- https://ris-live.ripe.net/

## License

Copyright © 2022 FIXME

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
