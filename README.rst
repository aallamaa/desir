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

#. Iterator

you can use object Counter from a Redis class instance as a unique counter across as
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

Result::
 the initial value of the counter is 5
 6
 7
 8
 9
 10
 11
 the next value of the counter is 12
