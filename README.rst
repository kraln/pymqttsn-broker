pymqttsn broker
===============

mqtt-sn broker based on the mqtt-sn 1.2 spec

who
---

Jeff Katz <kraln@kraln.com>, someone with way too much and yet somehow 
not enough time on their hands.

why
---

I was leading tech at a shop where the backend was written in Python,
and so when we decided to introduce a new, standards-based IoT protocol for
our devices and infrastructure, and chose MQTT-SN, we wanted our a broker
that fit with the rest of our system. 

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
My stuff is MPLv2. Other stuff is as per their files.
