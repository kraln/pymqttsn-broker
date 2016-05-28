pymqttsn broker
===============

who
--

KIWI.KI GmbH is a physical access control as a service provider based in
Berlin, Germany.

why
---

The KIWI backend is written in Python, and so when we decided
to introduce a new, standards-based IoT protocol for our devices and
infrastructure, and chose MQTT-SN, we wanted our a broker that fit with the
rest of our system. 

what
---
As there are very few broker implementations, and the
concept of the broker is very similar to our previous in-house prototcol,
we've decided to write a standards-compilant MQTT-SN broker in Python.

As we expect many, many devices connected to our system, all state is stored
in a redis cluster, allowing many broker processes to collaborate

how
---
Python 3, libuv, asyncio, and redis. and love.


license
-------
KIWI's stuff is MPLv2. Other stuff is as per their files.
