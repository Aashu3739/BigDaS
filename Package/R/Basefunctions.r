Is_Installed <- function(mypkg) {is.element(mypkg, installed.packages()[,1])}

Load_Or_Install<-function(package_names)
{
  for(package_name in package_names)
  {
    if(!Is_Installed(package_name))
    {
      #install.packages(package_name,repos="htttp://lib.stat.cmu.edu/R/CRAN")
      install.packages(package_name,repos="http://cran.us.r-project.org")
    }
    library(package_name,character.only=TRUE,quietly=TRUE,verbose=FALSE)
  }
}

`?` <- function(x, y) {
  print("x is");
  print(x);
  print("y is");
  print(y);
  xs <- as.list(substitute(x))
  if (xs[[1]] == as.name("<-")) x <- eval(xs[[3]])
  r <- eval(sapply(strsplit(deparse(substitute(y)), ":"), function(e) parse(text = e))[[2 - as.logical(x)]])
  if (xs[[1]] == as.name("<-")) {
    xs[[3]] <- r
    eval.parent(as.call(xs))
  } else {
    r
  }
}

NoOfDays<-function()
{
  return(strftime(Sys.Date(), "%j"))
}

FirstValue<-function(Value)
{ 
  return(Value[1])
}

LatestValue<-function(Value)
{
  if(length(Value)>0)
  {
    Value=Value[Value!=0];
    if(length(Value)>0){return(Value[length(Value)]);}
  }
  return(0);
}

WeightedAverage<-function(df)
{
  UTS=as.difftime(strftime(df[,"RECORDEDTIMESTAMP"],"%H:%M:%S"),units="secs")
  UTS=diff(UTS)
  UTS<-ifelse(UTS< 0,-UTS,UTS)
  Res=as.numeric(UTS*df[1:(nrow(df)-1),"VALUE"])
  Res=sum(Res)/sum(UTS);
  return(Res);
}

#UI value timetotal(ID)
TimeTotalized<-function(df)
{  
  UTS=as.difftime(strftime(df[,"RECORDEDTIMESTAMP"],"%H:%M:%S"),units="secs")
  UTS=diff(UTS)
  UTS<-ifelse(UTS< 0,-UTS,UTS)
  Res=as.numeric(UTS*df[1:(nrow(df)-1),"VALUE"])
  return(sum(Res));
}  

ZeroToOne<-function(ResultSet)
{
  ZDF=ResultSet[1:(length(ResultSet)-1)]
  LDF=ResultSet[-1]
  TotalFlip=sum(ZDF==0 & (ZDF|LDF==1))
  return(TotalFlip)
}

OneToZero<-function(ResultSet)
{
  ZDF=ResultSet[1:(length(ResultSet)-1)]
  LDF=ResultSet[-1]
  TotalFlip=sum(ZDF==1&(ZDF&LDF== 0))
  return(TotalFlip)
}

Cumulative<-function(Value)
{
  if(length(Value)>1){return(Value[length(Value)]-Value[1]);
  }else{ return(0);}
}

NoOccur<-function(Value)
{
  return(length(Value));
}

ExEVALHM<-function(df,ExpressionM,MPIDS)
{
  Res=c();
  ExpressionM=gsub("@[A-Z]*","",ExpressionM);
  ExpressionM=gsub("LML","HM",ExpressionM);
  ExpressionM=gsub("LM","HM",ExpressionM);
  for(E in 1:nrow(df))
  {
    exp=ExpressionM
    for(D in 1:length(MPIDS)){
      exp=gsub(paste("HM",colnames(df)[D+1],sep=""),df[E,D+1],exp,fixed=T)
    }
    Res[E]=eval(parse(text=paste0(exp)))
    if(isTRUE(all.equal(cos_d(90),Res)))
    {
      Res[E]=0
    }
    
  }
  return(Res);
}

ExEVAL<-function(df,ExpressionM,MPIDS)
{
  Res=c();
  ExpressionM=gsub("HM|LM|CM|@[A-Z]*","",ExpressionM);
  for(E in 1:nrow(df))
  {
    exp=ExpressionM
    for(D in 1:length(MPIDS)){
      MI=gregexpr("(?<![.\\d])\\d{4}(?![.\\d])",exp,perl=T)
      if(length(MI[[1]])>(length(MPIDS)-D+1))
      {
        count=0;
        for(Index in 1:length(MI[[1]]))
        {
          SMI=gregexpr("(?<![.\\d])\\d{4}(?![.\\d])",exp,perl=T)
          SI=SMI[[1]][Index-count]
          if(!is.na(match(MPIDS[D],substr(exp,SI,SI+3))))
          {
            exp=paste(substr(exp, 1,SI-1),df[E,D+1],substr(exp,(SI+4), nchar(exp)), sep='')
            count=count+1
          }
        }
        
      }else{
        #exp=paste(substr(exp, 1, MI[[1]][1]-1),df[E,D+1],substr(exp,(MI[[1]][1]+attr(MI[[1]], "match.length")[1]), nchar(exp)), sep='')
        exp=gsub(colnames(df)[D+1],df[E,D+1],exp,fixed=T)
      }
    }
    Res[E]=eval(parse(text=paste0(exp)))
    if(isTRUE(all.equal(cos_d(90),Res)))
    {
      Res[E]=0
    }
  }
  return(Res);
}

MeanNo0<-function(Value)
{
  return(mean(Value[Value!=0]));
}
