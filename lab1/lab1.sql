create database if mot exists lab1db;
use lab1db;
create table if not exists lab1_messages 
(
   timestamp_ Int64,
   referer_ String,
   location_ String,
   remoteHost_ String,
   partyId_ String,
   sessionId_ String,
   pageViewId_ String,
   eventType_ String,
   item_id_ String,
   item_price_ Decimal32(2),
   item_url_  String,
   basket_price_ Nullable(Decimal32(2)),   
   detectedDuplicate_ Enum8('true' = 1, 'false' = 0),
   detectedCorruption_  Enum8('true' = 1, 'false' = 0),
   firstInSession_ Enum8('true' = 1, 'false' = 0),
   userAgentName_ String
) ENGINE = Log;