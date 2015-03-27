This is a collection of examples for how to use libev with different languages.

Currently there is Chicken Scheme, Chapel, and C (for control?)

In all cases, interop is pretty straight forward and allows handling libev events in your language of choice.

The general architecture of the libev tcp server is to fork threads, each of which shares the common socket events and then uses its own event loop to do work.


SETUP
=====

General setup is expecting an OSX brew installation:

    brew install libev
    
Chicken scheme:

    brew install chicken

Chapel

    http://chapel.cray.com/download.html


COMPLING


    make



