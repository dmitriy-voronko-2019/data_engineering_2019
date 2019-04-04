use default;
create table if not exists dmitriy_voronko 
(
   timestamp Int64,
   referer String,
   location String,
   remoteHost String,
   partyId String,
   sessionId String,
   pageViewId String,
   eventType String,
   item_id String,
   item_price Decimal32(2),
   item_url  String,
   basket_price Nullable(Decimal32(2)),   
   detectedDuplicate Enum8('true' = 1, 'false' = 0),
   detectedCorruption  Enum8('true' = 1, 'false' = 0),
   firstInSession Enum8('true' = 1, 'false' = 0),
   userAgentName String
) ENGINE = Log;