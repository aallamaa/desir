=========================
Desir Redis Python Client
=========================
Attempt to make a minimalist redis python client.

Plus some nice pythonic stuff that will soon come.
(message passing, iterator, dict, set and list transparent implementation)



Install
=======

sudo python setup.py install


Pythonic sugar
==========

1. Iterator
--------

You can use object Counter from a Redis class instance as a unique counter across as
many process, thread, server you could potentially have providing they
can access the same redis instance/cluster.

>>> import desir
>>> r=desir.Redis()
>>> counter_name="counter" # the name of the counter is counter, meaning
>>> the key used inside redis is this name
>>> counter_seed=5 # intial value of the counter is 5
>>> c=r.Counter(counter_name,counter_seed)
>>> print "the initial value of the counter is", c
>>> for i in c:
...  print i
...  if i>10:
...   break
>>> print "the next value of the counter is", c.next()
 the initial value of the counter is 5
 6
 7
 8
 9
 10
 11
 the next value of the counter is 12
>>>

2. Connector
----------

A connector is an attempt to make a message passing interface similar
to the Erlang send / receive message passing functions.

A connector is defined by its name which is pointing internaly to a
redis list using the connector name as the key name inside redis.

Here is how it works:

On client "toto" you do this:
>>> import desir
>>> n=desir.Redis()
>>> c=n.Connector("toto",timeout=5)

On client "tata" you do the same:
>>> import desir
>>> n=desir.Redis()
>>> d=n.Connector("tata",timeout=5)

Note that the timeout defined when instanciating the connector is used
only when using the connector as an iterator. A value of 0 means
timeout is never reached.

Then let´s define on client "toto" an object to send to client
"tata". Note that you can send any serializable object (using pickle).

>>> v=[1,2,3,4,dict(a=2,b=3)]
>>> c.send("tata",v)
>>> c.send("tata",v)
>>> c.send("tata",v)

Now let´s go back to client "tata" and see the result:

>>> for v in d:
...  print v
... 
['toto', 1288551730.8449249, [1, 2, 3, 4, {'a': 2, 'b': 3}]]
['toto', 1288551730.8463399, [1, 2, 3, 4, {'a': 2, 'b': 3}]]
['toto', 1288551730.8468609, [1, 2, 3, 4, {'a': 2, 'b': 3}]]
>>> 

After 5 seconds, the for loop automatically stop trying to fetch
receive from the connector as timeout was defined as 5 (seconds).

You could also use d.receive() to get a result (blocking one) or
d.receive(timeout) for non blocking if timeout is not 0.
