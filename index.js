/**
 * Created by Parag on 14/08/15.
 */
var mongodb = require('mongodb');
var cleanup = require('./cleanup');
var transaction = require('./transaction.js')();

var MongoClient = mongodb.MongoClient;

var Q = require('q');
var url = 'mongodb://localhost:27017/mydb';




cleanup.Cleanup(function(){
   console.log("cleaning up");

})

MongoClient.connect(url, function (err, db) {

   transaction.init(db);

   transaction.rollback();


   transaction.startTransaction({collections:["mycollection","updateCollection"]},function(t){

       console.log("transaction started");

       t.collection("mycollection").insertOne({"data":"myval"},function(err,r){
           if(err){
               console.log(err);
           }
           else{
               console.log("success");
           }
       });

       t.collection("mycollection").insertOne({"data":"myval2"},function(err,r){

           t.error = true;

           if(err){
               console.log(err);
           }
           else{
               console.log("success");
           }
       });

       t.collection("updateCollection").updateOne({"foo":"bar"},{$set:{"updated":false}},function(err,result){

           if(err){
               console.log("errorAtUpdateCallback");
           }
           else
           console.log("updated record");
       })


      t.commit();

   });



})