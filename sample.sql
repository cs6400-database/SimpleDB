select * from Orders;
select * from Orders where ODate = '2011-05-21';
select * from Orders o1, OrderDetail o2 where o1.ODate = '2011-05-21' and o1.OID=o2.OID;
select o1.OID, (sum(o2.ONumber)) from Orders o1, OrderDetail o2 where o1.OID=o2.OID group by o1.OID;
set transaction read write;
insert into Orders values(46,107,'2018-12-02',50)
rollback;
set transaction read write;
insert into Orders values(46,107,'2018-12-02',50)
commit;
select * from Orders o1, OrderDetail o2, Customer c1, Item i1 where o1.OID=o2.OID and o1.CID=c1.CID and o2.ItemID=i1.ItemID;

