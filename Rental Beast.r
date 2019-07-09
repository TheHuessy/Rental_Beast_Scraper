library(httr)
library(bitops)
library(RCurl)
library(rvest)
library(XML)
library(xml2)
library(curl)
library(jsonlite)
library(civis)

##### RENTAL BEAST #####

rbd <- data.frame("brokerage_name" = as.character(),
                  "apartment_id" = as.character(),
                  "latitude" = as.character(),
                  "longitude" = as.character(),
                  "date_available" = as.character(),
                  "bedrooms" = as.character(),
                  "beds_count" = as.character(),
                  "bathrooms" = as.character(),
                  "rent" = as.character(),
                  "city" = as.character(),
                  "state" = as.character(),
                  "area" = as.character(),
                  "neighborhood" = as.character(),
                  "metro_id" = as.character(),
                  "neighborhood_id" = as.character(),
                  "sub_neighborhood_id" = as.character(),
                  "metro_name" = as.character(),
                  "nbhd_name" = as.character(),
                  "sub_nbhd_name" = as.character(),
                  "source" = as.character(),
                  "zip" = as.character(),
                  "address" = as.character(),
                  "apply_token_url" = as.character(),
                  "exclusive_by" = as.character(),
                  "mls_id" = as.numeric(),
                  "status" = as.character(),
                  "hide_rb_cp" = as.numeric(),
                  "hide_partner_cp" = as.character(),
                  "is_featured" = as.character(),
                  "match_type" = as.character(),
                  "Scrape_Date" = as.character()
)

zips <- {c("02118",
           "02119",
           "02120",
           "02130",
           "02134",
           "02135",
           "02445",
           "02446",
           "02447",
           "02467",
           "02108",
           "02114",
           "02115",
           "02116",
           "02215",
           "02128",
           "02129",
           "02150",
           "02151",
           "02152",
           "02124",
           "02126",
           "02131",
           "02132",
           "02136",
           "02109",
           "02110",
           "02111",
           "02113",
           "02121",
           "02122",
           "02124",
           "02125",
           "02127",
           "02210"
)}
for(i in 1:length(zips)){ 
  #Base Query up to page number
  brpl.1 <- "https://www.rentalbeast.com/api/listing.json?include_disclaimer=true&include_near_close_matches=true&include_neighborhood_filter_list=true&is_for_consumer=true&max_record_count="
  #then the max count value. We start with 500 and test if there are more and then rerun the call
  mc <- 500
  brpl.2 <- "&mls_toggle_value=mixed&page_number=1&partner_key=default&sort_ascending=true&sort_field=dateavailable&state=Massachusetts&statuses=Active&zip="
  #Then the zip value gets passed twice to the last two chunks of the call
  brpl.3 <- "&zip_codes="
  #The zip again
  rbl <- paste(brpl.1, mc, brpl.2, zips[i], brpl.3, zips[i], sep = "")
  
  #System sleep to keep from getting banned
  slp <- sample(1:6, 1)
  #print(paste("Sleeping for", slp, "seconds at", Sys.time()))
  Sys.sleep(slp)
  
  #Try catch to make sure the endpoint exists/we haven't been banned
  chka <-  tryCatch({
    fromJSON(rbl)
  },
  error = function(e){e}
  )
  
  if(inherits(chka, "error")) {
    print("URL Broken")
    next
  }
  
  #Another sleep, just in case
  slp <- sample(1:6, 1)
  print(paste("Sleeping for", slp, "seconds at", Sys.time()))
  Sys.sleep(slp)
  
  print(paste("PULLING LISTINGS FOR ZIP", zips[i],"--", i, "of", length(zips)))
  
  #Pull the data from the API endpoint
  rtu <- fromJSON(rbl)
  if (length(rtu$data) == 0){
    print("----")
    print(paste("NO DATA FOR ZIP", zips[i]))
    print("----")
    next
  }
  
  #check to see if we got all of the listings for that zip
  if (rtu$meta$total_records > mc){
    mc <- rtu$meta$total_records
    rbl <- paste(brpl.1, mc, brpl.2, zips[i], brpl.3, zips[i], sep = "")
    rtu <- fromJSON(rbl)
  }
  
  #Adding data to dataframe
  glsx <- data.frame("brokerage_name" = rtu$data$brokerage_name,
                     "apartment_id" = rtu$data$apartment_id,
                     "latitude" = rtu$data$latitude,
                     "longitude" = rtu$data$longitude,
                     "date_available" = rtu$data$date_available,
                     "bedrooms" = rtu$data$bedrooms,
                     "beds_count" = rtu$data$beds_count,
                     "bathrooms" = rtu$data$bathrooms,
                     "rent" =rtu$data$rent,
                     "city" = rtu$data$city,
                     "state" = rtu$data$state,
                     "area" = rtu$data$area,
                     "neighborhood" = rtu$data$neighborhood,
                     "metro_id" = rtu$data$metro_id,
                     "neighborhood_id" = rtu$data$neighborhood_id,
                     "sub_neighborhood_id" = rtu$data$sub_neighborhood_id,
                     "metro_name" = rtu$data$metro_name,
                     "nbhd_name" = rtu$data$nbhd_name,
                     "sub_nbhd_name" = rtu$data$sub_neighborhood_id,
                     "source" = rtu$data$source,
                     "zip" = rtu$data$zip,
                     "address" = rtu$data$address,
                     "apply_token_url" = rtu$data$apply_token_url,
                     "exclusive_by" = rtu$data$exclusive_by,
                     "mls_id" = as.numeric(rtu$data$mls_id),
                     "status" = rtu$data$status,
                     "hide_rb_cp" = rtu$data$hide_rb_cp,
                     "hide_partner_cp" = rtu$data$hide_partner_cp,
                     "is_featured" = rtu$data$is_featured,
                     "match_type" = rtu$data$match_type,
                     "Scrape_Date" = Sys.time()
  )
  
    
  #hide_rb_cp
  for (trr in 1:length(glsx$hide_rb_cp)){
    if (glsx$hide_rb_cp[trr] == FALSE){
      glsx$hide_rb_cp[trr] <- 0
    } else if (glsx$hide_rb_cp[trr] == TRUE){
      glsx$hide_rb_cp[trr] <- 1
    }
  }
  
  #hide_partner_cp
  for (trr in 1:length(glsx$hide_partner_cp)){
    if (glsx$hide_partner_cp[trr] == FALSE){
      glsx$hide_partner_cp[trr] <- 0
    } else if (glsx$hide_partner_cp[trr] == TRUE){
      glsx$hide_partner_cp[trr] <- 1
    }
  }
  
  
  
  #Adding new dataframe row to "master" dataframe  
  
  rbd <- rbind(rbd, glsx)
  
  print("++++")
  print(paste("SUCCESS PULLING LISTINGS FOR ZIP", zips[i],"--", i, "of", length(zips)))
  print("++++")
}

#Make sure there aren't any obvious duplicates
rbd <- unique(rbd)

#Write to main table
write_civis(rbd, tablename = "sandbox.rental_beast_master", database = "Boston", if_exists = "append")

#Write to daily table
write_civis(rbd, tablename = "sandbox.rental_beast_daily", database = "Boston", if_exists = "drop")
