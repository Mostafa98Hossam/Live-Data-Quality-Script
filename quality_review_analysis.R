`# This script is to put all quality edits and errors found in the live-offline-matching for the live collection in one table 
# showing the mistakes and what is supposed to be correct

# libraries and connections
#####

# libraries
library(RPostgreSQL)
library(tidyverse)
library(ggrepel)
library(DBI)
library(glue)
library(googlesheets4)
library(readxl)


gs4_deauth()
# live daily work google sheet
gs_dailywork <- googlesheets4::read_sheet(
  'https://docs.google.com/spreadsheets/d/1XQaPNUb-ML0p3ikOTQyOfsx4YKiAOwsmMSh6k9ptPa8/edit#gid=0',
  sheet='Daily Work 2022')
# live team codes
gs_teamcodes <- googlesheets4::read_sheet(
  'https://docs.google.com/spreadsheets/d/1XQaPNUb-ML0p3ikOTQyOfsx4YKiAOwsmMSh6k9ptPa8/edit#gid=0',
  sheet='Team Codes')

# athena database
#con_athena <- dbConnect(RAthena::athena(),
#                        aws_access_key_id = "AKIAUGFY3XC2CYCWROWT",
#                        aws_secret_access_key = "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",
#                        s3_staging_dir = "s3://statsbomb-athena-query-results-prod",
#                        region_name="eu-west-2")

# livecollectionservice database
con_live_collection <- dbConnect(dbDriver("PostgreSQL"),
                                 dbname = "livecollectionservice",
                                 host = "primary-db-ro.statsbomb.com",
                                 port = 5432,
                                 user = "live_ro", 
      password = "h9SNmQ6bA2")

# arqam database
con_offline <- dbConnect(dbDriver("PostgreSQL"),
                         dbname = "arqam_dev",
                         host = "arqam-db.statsbomb.com",
                         port = 5432,
                         user = "arqam_analytics", 
                         password = "drp528esvc")


# define time period to be queried from the database and store the matchids in an array so that they can be given as input to the 
# database queries
######

# filter live and quality reviewed matches
queryinput_matchids <- gs_dailywork %>% 
  filter(!is.na(Quality) & as.Date(`Match Date`) >= '2022-06-01' & `Live/Replay`=='Live' & Base != Quality & `Extras & F.F`!= Quality
         & Players != Quality & `Location & Impact` != Quality)  %>% distinct(MatchID)
# combine all matchids in one string array
queryinput_matchids_formatted  <- paste0("'",queryinput_matchids$MatchID,"',")
# remove comma after last string 
queryinput_matchids_formatted[length(queryinput_matchids_formatted)] <- 
  gsub('.{1}$', '',queryinput_matchids_formatted[length(queryinput_matchids_formatted)])
# convert array to one string to be used as input in SQL query
queryinput_matchids_formatted <- paste(queryinput_matchids_formatted,collapse = '')


# query databases and prepare tables 
#####

query_userids <- dbGetQuery(con_offline," SELECT u.id, u.hrcode FROM users u ")

Getbaseedits <- function (queryinput_matchids_formatted) {
  # get keys lastly edited by quality reviewer in type base and remove base events which were added and deleted by quality
  query <- glue("with base_edits as (select key, author, extract(epoch from \"capturedTime\"::timestamptz) as capturedTime,category,
payload::jsonb#>>'{name}'as eventname,payload::jsonb#>>'{videoTimestamp}' 
as eventvideotimestamp, payload::jsonb#>>'{fields}' as basefields, \"matchId\",\"type\",\"partId\",
payload::jsonb#>>'{teamId}' as teamId 
from match_events me 
where sport='football' and \"matchId\" in (*paste(queryinput_matchids_formatted)*) 
and type in ('base','deletion') and key in (
  select key from (
    select distinct on (me.\"key\") key, author from match_events me where me.\"matchId\" in  
    (*paste(queryinput_matchids_formatted)*)  and 
    type in ('base','deletion') order by key, me.\"capturedTime\" ::timestamptz desc) last_input_by_quality 
  where last_input_by_quality .author in (116,118,191,231,251,283,420,488)))
      select key,author,capturedTime,category,eventname,eventvideotimestamp,
      basefields,\"matchId\",\"type\",\"partId\",teamId from base_edits b1 
      where not exists (select key from base_edits b2 where b1.key=b2.key 
      and b1.category='base' and b2.type='deletion' and b1.author=b2.author)
              ",.open = "*", .close = "*")
  df <- dbGetQuery(con_live_collection,query)
  return(df)
}

query_baseedits <- Getbaseedits(queryinput_matchids_formatted)

Getplayersedits <- function (queryinput_matchids_formatted) {
  # get keys where players was last edited by quality
  query <- glue("select key,author,extract(epoch from \"capturedTime\"::timestamptz) as capturedTime,
category,type,payload::jsonb#>>'{players,playerId}' as playerId,payload::jsonb#>>'{players,secondaryPlayerId}' as secondaryplayerId,  
payload::jsonb#>>'{name}' as eventname, payload::jsonb#>>'{videoTimestamp}'
as eventvideotimestamp, me.\"matchId\", me.\"partId\", payload::jsonb#>>'{teamId}' as teamId
from match_events me 
where sport='football' and \"matchId\" in (*paste(queryinput_matchids_formatted)*)  and type in ('players','base') and key in (
  select key from (
    select distinct on (me.\"key\") key, author from match_events me where me.\"matchId\"in (*paste(queryinput_matchids_formatted)*) and 
    type in ('players') and category='amendment' order by key, me.\"capturedTime\" ::timestamptz desc) a
  where a.author in (116,118,191,231,251,283,420,488))",.open = "*", .close = "*")
  df <- dbGetQuery(con_live_collection,query)
  return(df)
}

query_playeredits <- Getplayersedits(queryinput_matchids_formatted)

Getextrasmistakes <- function (queryinput_matchids_formatted) {
  # get keys where extras was last edited by quality
  query <- glue("select key,author,
  extract(epoch from \"capturedTime\"::timestamptz) as capturedTime,
category,type,
case when type='extras' then payload::jsonb#>>'{fields,type}' else null end as extras_type ,
payload::jsonb#>>'{fields,height}' as extras_height, 
payload::jsonb#>>'{fields,body-part}' as extras_bodypart,
payload::jsonb#>>'{fields,extras}' as extras_extras, 
payload::jsonb#>>'{name}' as eventname, payload::jsonb#>>'{videoTimestamp}'
as eventvideotimestamp, me.\"matchId\", me.\"partId\", payload::jsonb#>>'{teamId}' as teamId
from match_events me 
where sport='football' and \"matchId\" in (*paste(queryinput_matchids_formatted)*)  
and type in ('extras','base') and key in (
  select key from (
    select distinct on (me.\"key\") key, author from match_events me where me.\"matchId\"in (*paste(queryinput_matchids_formatted)*) and 
    type in ('extras') and category='amendment' order by key, me.\"capturedTime\" ::timestamptz desc) a
  where a.author in (116,118,191,231,251,283,420,488))",.open = "*", .close = "*")
  df <- dbGetQuery(con_live_collection,query)
  return(df)
}

query_extrasedits <- Getextrasmistakes(queryinput_matchids_formatted)

Getlocationmistakes <- function (queryinput_matchids_formatted) {
  # get keys where location was last edited by quality
  query <- glue("select key,author,
extract(epoch from \"capturedTime\"::timestamptz) as capturedTime,
category,type,
payload::jsonb#>>'{location,actual,x}' as location_x ,
payload::jsonb#>>'{location,actual,y}' as location_y ,
payload::jsonb#>>'{name}' as eventname, payload::jsonb#>>'{videoTimestamp}'
as eventvideotimestamp, me.\"matchId\", me.\"partId\", payload::jsonb#>>'{teamId}' as teamId
from match_events me 
where sport='football' and \"matchId\"in (*paste(queryinput_matchids_formatted)*)
and type in ('location','base') and key in (
  select key from (
    select distinct on (me.\"key\") key, author from match_events me 
    where \"matchId\" in (*paste(queryinput_matchids_formatted)*) and 
    type in ('location') and category='amendment' order by key, me.\"capturedTime\"::timestamptz desc) a
  where a.author in (116,118,191,231,251,283,420,488))",.open = "*", .close = "*")
  df <- dbGetQuery(con_live_collection,query)
  return(df)
}

query_locationedits <- Getlocationmistakes(queryinput_matchids_formatted)

Getfreezeframemistakes <- function (queryinput_matchids_formatted) {
  # query gets freezeframe players count from collector and quality when they are different
  query <- glue("with collector as (select distinct on (me.\"key\") key, me.\"matchId\" , me.author, 
me.payload, 
me.\"capturedTime\" from match_events me 
where me.\"type\" in ('freeze-frame') and me.author not in (116,118,191,231,251,283,420,488) 
and me.\"matchId\"in (!paste(queryinput_matchids_formatted)!) 
order by me.\"key\", me.\"capturedTime\" desc),
quality as (select distinct on(me.\"key\") key as key_qa ,me.author, me.payload as payload_qa, 
me.\"capturedTime\" from match_events me 
where me.\"type\" in ('freeze-frame') and me.category='amendment' and me.author in (116,118,191,231,251,283,420,488) 
and me.\"matchId\"in (!paste(queryinput_matchids_formatted)!) 
order by me.\"key\", me.\"capturedTime\" desc),
qa_collector as (
select collector.\"matchId\", collector.author,collector.key, json_array_length(payload::json->'freezeFrame'->'players') as extras_collector
,json_array_length(payload_qa::json->'freezeFrame'->'players') as extras
from collector inner join quality on collector.\"key\"=quality.key_qa 
                where json_array_length(payload::json->'freezeFrame'->'players') <> 
                json_array_length(payload_qa::json->'freezeFrame'->'players'))
select distinct qa_collector.*,
                   me.payload::jsonb#>>'{videoTimestamp}' as eventvideotimestamp
                   ,me.\"partId\", me.payload::jsonb#>>'{teamId}' as teamId
                   from qa_collector left join match_events me on qa_collector.key=me.key 
               and me.payload::jsonb#>>'{videoTimestamp}' is not null
                ",.open = "!", .close = "!")
  
  df <- dbGetQuery(con_live_collection,query)
  return(df)
}

query_freezeframeedits <- Getfreezeframemistakes(queryinput_matchids_formatted)

Getgoallocationmistakes <- function (queryinput_matchids_formatted) {
  # query gets goallocation edits when quality changes it to more than 12 units in the x or y direction 
  query <- glue("with collector as (select distinct on (me.\"key\") key as key_collector, me.\"matchId\" , me.author, 
                     me.payload::jsonb#>>'{goal-location,x}' as location_x_collector, 
                     me.payload::jsonb#>>'{goal-location,y}' as location_y_collector,
                     me.\"capturedTime\" from match_events me 
                     where me.\"type\" in ('goal-location') and me.author not in (116,118,191,231,251,283,420,488) 
                     and me.\"matchId\" in (!paste(queryinput_matchids_formatted)!)
                     order by me.\"key\", me.\"capturedTime\" desc),
  quality as (select distinct on(me.\"key\") key as key ,me.author, me.payload::jsonb#>>'{goal-location,x}' as location_x, 
              me.payload::jsonb#>>'{goal-location,y}' as location_y, me.\"capturedTime\" from match_events me 
              where me.\"type\" in ('goal-location') and me.category='amendment' and me.author in (116,118,191,231,251,283,420,488) 
              and me.\"matchId\"in (!paste(queryinput_matchids_formatted)!) 
              order by me.\"key\", me.\"capturedTime\" desc),
  qa_collector as (select collector.key_collector, quality.key, collector.\"matchId\", collector.location_x_collector, 
  collector.location_y_collector, collector.author,
  quality.location_x, quality.location_y 
  from collector inner join quality on quality.\"key\"=collector.key_collector where 
                   abs(location_x::int - location_x_collector::int )>12 or abs(location_y::int - location_y_collector::int )>12)
                   select distinct qa_collector.*,
                   me.payload::jsonb#>>'{videoTimestamp}' as eventvideotimestamp
                   ,me.\"partId\", me.payload::jsonb#>>'{teamId}' as teamId
                   from qa_collector left join match_events me on qa_collector.key=me.key 
               and me.payload::jsonb#>>'{videoTimestamp}' is not null",.open = "!", .close = "!")
  
  df <- dbGetQuery(con_live_collection,query)
  return(df)
}

query_goallocationedits <- Getgoallocationmistakes(queryinput_matchids_formatted)

Getimpactmistakes <- function (queryinput_matchids_formatted) {
  # query gets impact edits when quality changes it to more than 10 units in the x direction
  query <- glue("with collector as (select distinct on (me.\"key\") key as key_collector, me.\"matchId\" , me.author, 
me.payload::jsonb#>>'{impact}' as location_x_collector, 
me.\"capturedTime\" from match_events me 
where me.\"type\" in ('impact') and me.author not in (116,118,191,231,251,283,420,488) 
and me.\"matchId\"in (!paste(queryinput_matchids_formatted)!) 
order by me.\"key\", me.\"capturedTime\" desc),
quality as (select distinct on(me.\"key\") key as key , me.payload::jsonb#>>'{impact}' as location_x, 
me.\"capturedTime\" from match_events me 
where me.\"type\" in ('impact') and me.category='amendment' and me.author in (116,118,191,231,251,283,420,488) 
and me.\"matchId\"in (!paste(queryinput_matchids_formatted)!) 
order by me.\"key\", me.\"capturedTime\" desc),
qa_collector as (select collector.key_collector, quality.key, collector.\"matchId\", collector.location_x_collector, collector.author,
  quality.location_x 
  from collector inner join quality on quality.\"key\"=collector.key_collector where 
                   abs(location_x::float - location_x_collector::float )>10) 
                   select distinct qa_collector.*,
                   me.payload::jsonb#>>'{videoTimestamp}' as eventvideotimestamp
                   ,me.\"partId\", me.payload::jsonb#>>'{teamId}' as teamId
                   from qa_collector left join match_events me on qa_collector.key=me.key 
               and me.payload::jsonb#>>'{videoTimestamp}' is not null",.open = "!", .close = "!")
  
  df <- dbGetQuery(con_live_collection,query)
  return(df)
}

query_impactedits <- Getimpactmistakes(queryinput_matchids_formatted)

df_hrcode_nickname_id <- merge(gs_teamcodes,query_userids, by.x="Code", by.y="hrcode") %>% filter(!is.na(Code))

df_basedits <- inner_join(query_baseedits,gs_dailywork %>% select(MatchID,Quality), by=c("matchId"="MatchID")) 
df_basedits <- inner_join(df_basedits,df_hrcode_nickname_id  %>% select(Name,id) , by =c("Quality"="Name"))

df_playeredits <- inner_join(query_playeredits,gs_dailywork %>% select(MatchID,Quality), by=c("matchId"="MatchID")) 
df_playeredits <- inner_join(df_playeredits,df_hrcode_nickname_id  %>% select(Name,id) , by =c("Quality"="Name"))

df_extrasedits <- inner_join(query_extrasedits,gs_dailywork %>% select(MatchID,Quality), by=c("matchId"="MatchID")) 
df_extrasedits <- inner_join(df_extrasedits,df_hrcode_nickname_id  %>% select(Name,id) , by =c("Quality"="Name"))

df_freezeframeedits <- inner_join(query_freezeframeedits,gs_dailywork %>% select(MatchID,Quality), by=c("matchId"="MatchID")) 
df_freezeframeedits <- inner_join(df_freezeframeedits,df_hrcode_nickname_id  %>% select(Name,id) , by =c("Quality"="Name"))

df_locationedits <- inner_join(query_locationedits,gs_dailywork %>% select(MatchID,Quality), by=c("matchId"="MatchID")) 
df_locationedits <- inner_join(df_locationedits,df_hrcode_nickname_id  %>% select(Name,id) , by =c("Quality"="Name"))

df_goallocationedits <- inner_join(query_goallocationedits,gs_dailywork %>% select(MatchID,Quality), by=c("matchId"="MatchID")) 
df_goallocationedits <- inner_join(df_goallocationedits,df_hrcode_nickname_id  %>% select(Name,id) , by =c("Quality"="Name"))

df_impactedits <- inner_join(query_goallocationedits,gs_dailywork %>% select(MatchID,Quality), by=c("matchId"="MatchID")) 
df_impactedits <- inner_join(df_goallocationedits,df_hrcode_nickname_id  %>% select(Name,id) , by =c("Quality"="Name"))

# wrangle base edits table 
#####

# Quality Reviewer Events Amendments
df_basedits_qualityreviewer_amendments  <- df_basedits %>% 
  group_by(key) %>% filter(author==id & category == 'amendment') %>%
  slice_max(capturedtime)

# Quality Reviewer Events Added
df_basedits_qualityreviewer_events <- df_basedits %>% 
  group_by(key) %>% filter(author==id & category != 'amendment') %>%
  slice_max(capturedtime) 

# Last Key Input by Collector
df_basedits_collector <- df_basedits %>% 
  group_by(key) %>% filter(author!=id) %>%
  slice_max(capturedtime) 

# Amendments Edits - inner join keys - key with last basefield amendment by quality vs last basefield input by collector
df_basedits_basefields <- merge(df_basedits_qualityreviewer_amendments,df_basedits_collector,
                             by.x=c('key'),by.y=c('key')) %>% 
  filter(basefields.x != basefields.y) %>% rename(basefields=basefields.x, basefields_collector=basefields.y) %>% 
  select(key,eventvideotimestamp.x,eventname.x,
      eventvideotimestamp.y,eventname.y,author.x,matchId.x,partId.x, teamid.x,teamid.y, Quality.x, basefields, basefields_collector) %>%
  rename(Quality=Quality.x, teamid=teamid.x,teamid_collector=teamid.y, eventvideotimestamp=eventvideotimestamp.x, eventname=eventname.x, 
         key_collector=key,eventvideotimestamp_collector=eventvideotimestamp.y, eventname_collector=eventname.y,
         author=author.x, matchId=matchId.x, partId=partId.x) %>% mutate(key=key_collector)

# Deleted and Added 
# full join on matchid and partid all keys added by quality - last input added by collector
# filter out rows which are more different with more than 0.5s in videotimesamp value or have the same eventname
# every key from quality should be only joined with one key from collector and vice versa 
df_basedits_deletedandadded <- merge(df_basedits_qualityreviewer_events,df_basedits_collector, 
                             by.x=c('matchId','partId'), by.y=c('matchId','partId')) %>% 
  mutate(diff=abs(as.numeric(eventvideotimestamp.x)-as.numeric(eventvideotimestamp.y))) %>% filter(diff<500 & eventname.x != eventname.y) %>%
  group_by(key.x) %>% 
  slice_min(diff) %>% 
  group_by(key.y) %>%
  slice_min(diff) %>%
  select(key.x,eventvideotimestamp.x,eventname.x,
         key.y,eventvideotimestamp.y,eventname.y,author.x,matchId,partId, teamid.x,teamid.y, Quality.x) %>%
  rename(author=author.x,Quality=Quality.x,teamid=teamid.x,teamid_collector=teamid.y,key=key.x, eventvideotimestamp=eventvideotimestamp.x, eventname=eventname.x, key_collector=key.y,eventvideotimestamp_collector=eventvideotimestamp.y, eventname_collector=eventname.y) %>%
  mutate(basefields=NA,basefields_collector=NA)

# Events Added by Quality and Missing by Collector
df_baseedits_added <- df_basedits_qualityreviewer_events %>% filter(!(key %in% df_basedits_deletedandadded$key)) %>% 
  mutate(basefields=NA,basefields_collector=NA,eventname_collector='missing', eventvideotimestamp_collector=NA, key_collector=NA, teamid_collector=NA)

df_baseedits_added <- inner_join(df_baseedits_added, gs_dailywork %>% select(MatchID,Base), by=c("matchId"="MatchID")) 

df_baseedits_added <- inner_join(df_baseedits_added, df_hrcode_nickname_id  %>% select(Name,id), by =c("Base"="Name")) %>%
  mutate(author=id.y) %>% select(-id.x,-id.y, -Base, -capturedtime, -category,-type)


# Events Extra inserted by Collector and Deleted by Quality
df_keys_deleted_from_quality <- df_basedits_qualityreviewer_amendments %>% filter (is.na(basefields)) %>% select(key)

df_baseedits_deletions <- df_basedits_collector %>% 
  filter(!(key %in% df_basedits_deletedandadded$key_collector) & !(key %in% df_basedits_basefields$key_collector) & 
           (key %in% (df_keys_deleted_from_quality$key) )) %>% 
  group_by(key) %>% 
  slice_max(capturedtime)  %>% ungroup() %>% select(key,author,eventname,eventvideotimestamp,basefields, matchId, partId, teamid, Quality) %>% 
  rename(key_collector=key, eventname_collector=eventname, eventvideotimestamp_collector=eventvideotimestamp, 
         basefields_collector=basefields, teamid_collector=teamid)  %>% 
  mutate(eventname='deleted',key=NA,eventvideotimestamp=NA,teamid=NA, basefields=NA)

# All Base Mistakes detected by Quality Reviewer 
df_allbaseedits <- rbind(df_basedits_basefields, df_basedits_deletedandadded, df_baseedits_added, df_baseedits_deletions) %>% distinct() %>% 
  mutate(error='base')

# wrangle players edits table 
#####

# base info for event
df_playeredits_baseinfo <- df_playeredits %>% filter(type=='base') %>% group_by(key) %>% 
  slice_max(capturedtime) %>% select(key,eventname,eventvideotimestamp, partId, teamid)

# last input by quality
df_playeredits_qualityreviewer <- df_playeredits%>% filter(author==id) %>% group_by(key) %>% 
  slice_max(capturedtime) %>% select(-author)

# last input by collector
df_playeredits_collector <- df_playeredits %>% filter (author!=id) %>% group_by(key) %>% slice_max(capturedtime) %>% 
  select(author,key, playerid,secondaryplayerid, teamid)

# playerid as given by collector vs playerid as given by quality reviewer
df_playeredits_collector_qualityreviewer <- inner_join(df_playeredits_qualityreviewer,df_playeredits_collector, by =c("key"="key")) %>% distinct() %>% 
  rename(playerid=playerid.x, secondaryplayerid=secondaryplayerid.x, 
         playerid_collector=playerid.y, secondaryplayerid_collector=secondaryplayerid.y) %>% select(-teamid.x,-teamid.y)

# create a row for primary and a row for secondary wrong players
df_playeredits_primaryplayer <- df_playeredits_collector_qualityreviewer %>% filter(playerid != playerid_collector) %>%
  mutate(player_type = 'primary') %>% select(-secondaryplayerid,-secondaryplayerid_collector, -capturedtime, -eventname, -eventvideotimestamp, 
                                             -partId, -category, -id, -type)

df_playeredits_secondaryplayer <- df_playeredits_collector_qualityreviewer %>% filter(!is.na(secondaryplayerid_collector) & 
                                                         !is.na(secondaryplayerid) & secondaryplayerid != secondaryplayerid_collector) %>%
  mutate(player_type = 'secondary') %>% select(-playerid,-playerid_collector, -capturedtime, -eventname, -eventvideotimestamp, 
                                               -partId, -category, -id, -type) %>% 
  rename(playerid=secondaryplayerid, playerid_collector=secondaryplayerid_collector)


df_allplayeredits <- inner_join(rbind(df_playeredits_primaryplayer, df_playeredits_secondaryplayer),df_playeredits_baseinfo,by =c("key"="key"))  %>% distinct() %>%  
  mutate(error='players',eventname_collector=eventname, key_collector=key, 
         eventvideotimestamp_collector=eventvideotimestamp, teamid_collector=teamid)

# wrangle extras edits table 
#####

# base info for event
df_extrasedits_baseinfo <- df_extrasedits %>% filter(type=='base') %>% group_by(key) %>% 
  slice_max(capturedtime) %>% select(key,eventname,eventvideotimestamp, partId, teamid)

# last input by quality
df_extrasedits_qualityreviewer <- df_extrasedits %>% filter(author==id) %>% group_by(key) %>% 
  slice_max(capturedtime) %>% select(-author, -teamid)

# last input by collector
df_extrasedits_collector <- df_extrasedits %>% filter (author!=id) %>% 
  group_by(key) %>% slice_max(capturedtime) %>% 
  select(author,key, extras_bodypart,extras_extras,extras_height,extras_type)

# extras as given by collector vs extras as given by quality reviewer
df_extrasedits_collector_qualityreviewer <- inner_join(df_extrasedits_qualityreviewer, df_extrasedits_collector, by =c("key"="key")) %>% distinct() %>% 
  rename(extras_bodypart=extras_bodypart.x, extras_extras=extras_extras.x, extras_height=extras_height.x, extras_type=extras_type.x, 
         extras_bodypart_collector=extras_bodypart.y, extras_extras_collector=extras_extras.y, extras_height_collector=extras_height.y, extras_type_collector=extras_type.y)

df_extrasedits_bodypart <- df_extrasedits_collector_qualityreviewer %>% 
  filter(extras_bodypart!=extras_bodypart_collector) %>%
  mutate(extras_error = 'bodypart') %>% 
  select(-extras_extras,-extras_extras_collector, -extras_height, -extras_height_collector, 
         -extras_type, -extras_type_collector) %>% rename(extras=extras_bodypart, extras_collector=extras_bodypart_collector)

df_extrasedits_height <- df_extrasedits_collector_qualityreviewer %>% 
  filter(extras_height!=extras_height_collector) %>%
  mutate(extras_error = 'height') %>% 
  select(-extras_extras,-extras_extras_collector, -extras_bodypart, 
         -extras_bodypart_collector, -extras_type, -extras_type_collector) %>% 
  rename(extras=extras_height, extras_collector=extras_height_collector)

df_extrasedits_extras <- df_extrasedits_collector_qualityreviewer %>% 
  filter(extras_extras!=extras_extras_collector) %>%
  mutate(extras_error = 'extras') %>% 
  select(-extras_bodypart, -extras_bodypart_collector, 
         -extras_type, -extras_type_collector, -extras_height, -extras_height_collector) %>% 
  rename(extras=extras_extras, extras_collector=extras_extras_collector)

df_extrasedits_type <- df_extrasedits_collector_qualityreviewer %>% 
  filter(extras_type!=extras_type_collector) %>%
  mutate(extras_error = 'type') %>% 
  select(-extras_bodypart, -extras_bodypart_collector, 
         -extras_extras, -extras_extras_collector, -extras_height, -extras_height_collector) %>% 
  rename(extras=extras_type, extras_collector=extras_type_collector)

df_allextrasedits <- inner_join(rbind(df_extrasedits_bodypart, df_extrasedits_height, df_extrasedits_extras, df_extrasedits_type) %>% 
                                  select(-eventname,-eventvideotimestamp, -partId,-capturedtime, -category, -type,-id),
                                df_extrasedits_baseinfo,by =c("key"="key") )%>% distinct() %>%
  mutate(error='extras',eventname_collector=eventname, key_collector=key, 
         eventvideotimestamp_collector=eventvideotimestamp)

# wrangle freeze-frame edits table 
#####

df_allfreezeframeedits <- df_freezeframeedits %>% mutate(eventname='shot', eventname_collector='shot', eventvideotimestamp_collector=eventvideotimestamp, 
                          teamid_collector=teamid, error='extras', extras_error='freeze-frame', key_collector=key, 
                          extras=as.character(extras), extras_collector=as.character(extras_collector)) %>% select(-id)

# wrangle location edits table 
#####

# base info for event
df_locationedits_baseinfo <- df_locationedits %>% filter(type=='base') %>% group_by(key) %>% 
  slice_max(capturedtime) %>% select(key,matchId,eventname,eventvideotimestamp, partId, teamid, Quality)

# last input by quality
df_locationedits_qualityreviewer <- df_locationedits %>% filter(author==id) %>% group_by(key) %>% 
  slice_max(capturedtime) %>% select(key, location_x, location_y)

# last input by collector
df_locationedits_collector <- df_locationedits %>% filter (author!=id) %>% 
  group_by(key) %>% slice_max(capturedtime) %>% 
  select(author,key, location_x,location_y)

df_location_collector_qualityreviewer <- inner_join(df_locationedits_qualityreviewer, df_locationedits_collector, 
                                                       by =c("key"="key")) %>% distinct() %>% 
  rename(location_x=location_x.x, location_y=location_y.x, 
         location_x_collector=location_x.y, location_y_collector=location_y.y) %>% filter(abs(as.numeric(location_x) - as.numeric(location_x_collector))>5 | 
                                                                    abs(as.numeric(location_y) - as.numeric(location_y_collector))>5 )

df_alllocationedits <- inner_join(df_location_collector_qualityreviewer, df_locationedits_baseinfo,by =c("key"="key")) %>% 
  mutate(error='location',eventname_collector=eventname, key_collector=key, 
         eventvideotimestamp_collector=eventvideotimestamp,
         teamid_collector=teamid)

# wrangle goal-location edits table 
##### 

df_allgoallocationedits <- df_goallocationedits %>% 
  mutate(error='location', eventname='goal_location', eventname_collector=eventname, 
         eventvideotimestamp_collector=eventvideotimestamp, teamid_collector=teamid) %>% select(-id)

# wrangle impact edits table 
#####

df_allimpactedits <- df_impactedits %>% 
  mutate(error='location', eventname='impact', eventname_collector=eventname, 
         eventvideotimestamp_collector=eventvideotimestamp, teamid_collector=teamid, location_y=NA, location_y_collector=NA) %>% select(-id.x,-id.y)

# combine all quality edits
#####

df_alledits<- bind_rows(df_allbaseedits, 
                    df_allextrasedits, df_allfreezeframeedits, 
                    df_allgoallocationedits, df_alllocationedits, df_allplayeredits, df_allimpactedits)

df_qualitymistakes_from_edits <- df_alledits %>% group_by(matchId,error) %>% 
  summarise(errors=n()) %>% spread(error, errors) %>% 
  rename(base_errors=base, extras_errors=extras, location_errors=location, players_errors=players)

df_qualitymistakes_from_edits[is.na(df_qualitymistakes_from_edits)] <- 0


# live/offline mistakes
#######################

df_liveoffline<-dbGetQuery(con_offline,"SELECT lorl.* FROM bi.live_offline_resolution_logs lorl 
left join matches m on m.id=lorl.offline_match_id where m.match_date >='2022-06-01'")

liveoffline_matchids <- df_liveoffline %>% distinct(live_match_id)
# combine all matchids in one string array
liveoffline_matchids_formatted  <- paste0("'",liveoffline_matchids$live_match_id,"',")
# remove comma after last string 
liveoffline_matchids_formatted[length(liveoffline_matchids_formatted)] <- 
  gsub('.{1}$', '',liveoffline_matchids_formatted[length(liveoffline_matchids_formatted)])
# convert array to one string to be used as input in SQL query
liveoffline_matchids_formatted <- paste(liveoffline_matchids_formatted,collapse = '')

get_live_videotimestamps <- function (live_match_id) {
  query <- glue( conn=con_live_collection,"select distinct payload::jsonb->>'videoTimestamp' as live_videotimestamp, me.\"key\"
from match_events me 
where me.\"matchId\" in (*paste(live_match_id)*) and me.type='base'",.open = "*", .close = "*")
  videotimestamps <- dbGetQuery(con_live_collection,query)
  videotimestamps$live_videotimestamp<-as.numeric(videotimestamps$live_videotimestamp)
  return(videotimestamps)
} 

videotimestamps <- get_live_videotimestamps(liveoffline_matchids_formatted)

df_liveoffline_withvideotimestamps <- left_join(df_liveoffline,videotimestamps, by=c("live_id"="key"))

# base
#######

# filter for base mistakes from liveoffline 
df_liveoffline_base <- df_liveoffline_withvideotimestamps %>% 
  filter(different_type==1 & Re_diff_type %in% c('Offline right','Both are wrong') & 
                                                 !(live_type %in% c('Ground Pass','High Pass','Low Pass') &
                                                     offline_type %in% c('Ground Pass','High Pass','Low Pass')) & 
                                                 !(live_type == 'Dispossessed' | offline_type == 'Dispossessed')) %>%
  rename(matchId=live_match_id, key=live_id, teamid=team_id, partId=part_id, eventname=live_type) %>% 
  mutate(author=live_base_collector, Quality='live/offline',
         liveoffline_base_resolution=ifelse(Re_diff_type=='Offline right',offline_type,'unknown'), error='base',
         eventvideotimestamp_collector=live_videotimestamp) %>%
  select(matchId,key,teamid,partId,eventname,liveoffline_base_resolution, author,Quality, error, eventvideotimestamp_collector)  

df_liveoffline_extras <- df_liveoffline_withvideotimestamps %>% filter(different_type==1 & Re_diff_type %in% c('Offline right','Both are wrong') &
                                                                        ( ( live_type %in% c('Ground Pass','High Pass','Low Pass') &
                                                                             offline_type %in% c('Ground Pass','High Pass','Low Pass') ) |
                                                                         (live_type == 'Dispossessed' | offline_type == 'Dispossessed') ) ) %>%
  rename(matchId=live_match_id, key=live_id, teamid=team_id, partId=part_id, eventname=live_type) %>% 
  mutate(author=live_base_collector, Quality='live/offline',
         liveoffline_base_resolution=ifelse(Re_diff_type=='Offline right',offline_type,'unknown'), error='extras',
         eventvideotimestamp_collector=live_videotimestamp) %>%
  select(matchId,key,teamid,partId,eventname,liveoffline_base_resolution, author,Quality, error, eventvideotimestamp_collector) 

# base mistakes by quality reviewer and collector
df_base_wrong_in_edits_liveoffline <- left_join(df_alledits %>% 
                                                  filter(key %in% df_liveoffline_base$key & error=='base'),df_liveoffline_base %>% 
                                                  select(key,liveoffline_base_resolution), by=c("key"="key")) %>% mutate(Quality='edited and changed by live/offline')

# combine base mistakes from quality edits, base mistakes from liveoffline and base mistakes from both
df_all_base_mistakes <- bind_rows(df_alledits %>% filter(error=='base' & 
                                                       !(key %in% df_base_wrong_in_edits_liveoffline)) %>% 
                                    mutate(across(everything(), as.character)),
                              df_base_wrong_in_edits_liveoffline %>% mutate(across(everything(), as.character)) ,df_liveoffline_base %>% 
                                mutate(key_collector=key,eventname_collector=eventname, 
                                       eventvideotimestamp=eventvideotimestamp_collector, teamid_collector=teamid) %>% 
                                filter(!(key %in% df_base_wrong_in_edits_liveoffline))%>% mutate(across(everything(), as.character)))
# players
#########

# filter for players mistakes
df_liveoffline_players_jersey <- df_liveoffline_withvideotimestamps %>% 
  filter(different_player==1 & Re_diff_player %in% c('Offline right','Both are wrong'))

# add playerid to jersey_number
from_jersey_to_playerids <- function (liveoffline_matchids_formatted) {
query <- glue("SELECT match_id,team_id,player_id, jersey_number from match_players mp 
                                   where mp.match_id in (*liveoffline_matchids_formatted*)",.open = "*", .close = "*")
df <- dbGetQuery(con_offline,query)

return (df)
}

df_from_jersey_to_playerids <- from_jersey_to_playerids(liveoffline_matchids_formatted)

df_liveoffline_players_withliveplayerid <- left_join(df_liveoffline_players_jersey,
                                                 df_from_jersey_to_playerids, 
                                                 by=c("live_jersey"="jersey_number", "team_id"="team_id", "live_match_id"="match_id"))

df_liveoffline_players_withallplayerids <- left_join(df_liveoffline_players_withliveplayerid,
                                                     df_from_jersey_to_playerids %>% rename(offline_playerid=player_id), 
                                                     by=c("offline_jersey_number"="jersey_number", 
                                                          "team_id"="team_id", "live_match_id"="match_id"))

# player mistakes from liveoffline
df_liveoffline_players <- df_liveoffline_players_withallplayerids %>%  rename(matchId=live_match_id, key=live_id, 
                                                                          teamid=team_id, partId=part_id, playerid=player_id, eventname=live_type) %>% 
  mutate(author=live_players_collector, Quality='live/offline',
         liveoffline_players_resolution=ifelse(Re_diff_player=='Offline right',offline_playerid,'unknown'), error='players', eventvideotimestamp_collector=live_videotimestamp) %>%
  select(matchId,key,teamid,partId,playerid,liveoffline_players_resolution, author,Quality, eventname, eventvideotimestamp_collector) 

# player mistakes by quality reviewer and collector
df_players_wrong_in_edits_liveoffline <- left_join(df_alledits %>% filter( error=='players' & key %in% df_liveoffline_players$key),
                                                   df_liveoffline_players %>% 
                                                  select(key,liveoffline_players_resolution), by=c("key"="key")) %>% mutate(Quality='edited and changed by live/offline')

# combine players mistakes from quality edits, players mistakes from liveoffline and players mistakes from both
df_all_players_mistakes <- bind_rows(df_alledits %>% filter(error=='players' & 
                                                          !(key %in% df_players_wrong_in_edits_liveoffline$key))
                                     %>% mutate(across(everything(), as.character) ),
                              df_players_wrong_in_edits_liveoffline %>% mutate(across(everything(), as.character) ),df_liveoffline_players %>% 
                                mutate(teamid=as.character(teamid), playerid_collector=as.character(playerid),playerid=NA,eventname_collector=eventname, 
                                       eventname=NA) %>% 
                                filter(!(key %in% df_players_wrong_in_edits_liveoffline$key))%>% mutate(error='players',across(everything(), as.character)))

#location
##########
# filter for location mistakes
df_liveoffline_location <- df_liveoffline_withvideotimestamps %>% 
  filter(different_location==1 & Re_diff_location %in% c('Offline right','Both are wrong')) %>%  
  rename(matchId=live_match_id, key=live_id, teamid=team_id, partId=part_id, location_x=live_x, location_y = live_y ,eventname=live_type) %>% 
  mutate(author=live_location_collector, Quality='live/offline',
         liveoffline_location_resolution=ifelse(Re_diff_location=='Offline right',paste0(as.character(offline_x), ':' ,as.character(offline_y)),'unknown'), 
         error='location', eventvideotimestamp_collector=live_videotimestamp) %>%
  select(matchId,key,teamid,partId,location_x,location_y,liveoffline_location_resolution, author,Quality, eventname, eventvideotimestamp_collector) 

# location mistakes by quality reviewer and collector
df_location_wrong_in_edits_liveoffline <- left_join(df_alledits %>% 
                                                      filter(error=='location' & 
                                                               key %in% df_liveoffline_location$key),
                                                   df_liveoffline_location %>% 
                                                     select(key,liveoffline_location_resolution), by=c("key"="key")) %>% mutate(Quality='edited and changed by live/offline')

# combine players mistakes from quality edits, players mistakes from liveoffline and players mistakes from both
df_all_location_mistakes <- bind_rows(df_alledits %>% mutate(across(everything(), as.character)) %>% 
                                        filter(error=='location' & !(key %in% df_location_wrong_in_edits_liveoffline)),
                                 df_location_wrong_in_edits_liveoffline %>% mutate(across(everything(), as.character)) ,df_liveoffline_location %>% 
                                   mutate(across(everything(), as.character), location_x_collector=location_x,
                                          location_y_collector =location_y,
                                          location_x=NA,location_y=NA,eventname_collector=eventname, 
                                          eventname=NA) %>% 
                                   filter(!(key %in% df_location_wrong_in_edits_liveoffline)) ) %>% mutate(error='location')

# combine all mistakes
######################

df_allmistakes<-bind_rows(df_all_base_mistakes , df_all_location_mistakes, df_all_players_mistakes,df_liveoffline_extras %>% mutate(across(everything(), as.character)),df_alledits %>% filter(error=='extras') %>% mutate(across(everything(), as.character)))

###################################################################################################################################################################################################################################################################

# prepare for tableau
#####################


Getplayerspositionsandnames <- function (queryinput_matchids_formatted) {
  query <- glue("select distinct on  (p.id,mp.match_id) p.id,mp.match_id, p.\"name\" as player_name ,p2.\"name\" as player_position_name  
from players p left join match_position mp on p.id=mp.player_id  
left join positions p2 on mp.player_position_id=p2.id 
                order by p.id,mp.match_id ,mp.player_position_id ",.open = "!", .close = "!")
  
  df <- dbGetQuery(con_offline,query)
  return(df)
}

df_playerpositionsandnames <- Getplayerspositionsandnames(queryinput_matchids_formatted) 

df_allmistakes_withplayersnamesandpositions <- left_join(
  df_allmistakes,df_playerpositionsandnames %>%
    mutate(across(everything(), as.character)), by=c("playerid"="id","matchId"="match_id")) %>% distinct()

df_allmistakes_withplayersnamesandpositions <- left_join(
  df_allmistakes_withplayersnamesandpositions,df_playerpositionsandnames %>% mutate(across(everything(), as.character)), 
  by=c("playerid_collector"="id","matchId"="match_id"))

df_allmistakes_withplayersnamesandpositions <- left_join(
  df_allmistakes_withplayersnamesandpositions,df_playerpositionsandnames %>% mutate(across(everything(), as.character)), by=c("liveoffline_players_resolution"="id","matchId"="match_id")) 

df_allmistakes_withplayersnamesandpositions <- df_allmistakes_withplayersnamesandpositions %>% rename(player_name_collector=player_name.y, 
                                                       player_position_collector=player_position_name.y,
                                                       player_name_liveoffline=player_name, 
                                                       player_position_liveoffline=player_position_name,
                                                       player_name=player_name.x,
                                                       player_position_name=player_position_name.x
                                                       )

file_name = paste0('D:/quality_edits.csv')
write.csv(x = df_allmistakes_withplayersnamesandpositions, file = file_name, row.names = FALSE)

# quality scores table
#######################

quality_scores <- df_allmistakes %>% 
  mutate(quality_type=ifelse(Quality %in% c('live/offline','edited and changed by live/offline'),Quality,'edited')) %>%
  group_by(matchId,error,quality_type) %>% distinct(key,key_collector) %>% summarise(errors=n()) %>%
  pivot_wider(names_from = c("error","quality_type"),values_from = errors)
  

quality_scores[is.na(quality_scores)] <- 0

file_name = paste0('D:/quality_edits_scores.csv')

write.csv(x = quality_scores, file = file_name, row.names = FALSE)
