WriteF<-function(DPH,RowkeyLatest,userDB,passDB,DBName,DBHost)
        {
          colnames(DPH)=c("ROWKEY","MODELPARAMETERID","EQUIPMENTID","VALUE","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP")
          DPL=data.frame() 
          #Index=suppressWarnings(is.finite(as.numeric(levels(DPH[,"VALUE"]))[DPH[,"VALUE"]]))
          #DPH=DPH[Index,]
          Index=suppressWarnings(is.finite(as.numeric(DPH[,"VALUE"])));
          DPH[!Index,"VALUE"]=0;
          if(nrow(DPH)>0)
          {
            DPL=data.frame(cbind(RowkeyLatest,DPH[nrow(DPH),2:ncol(DPH)]) ,stringsAsFactors=F)
            colnames(DPL)=c("ROWKEY","MODELPARAMETERID","EQUIPMENTID","VALUE","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP")
            TS=Sys.time()
            TS=gsub(" |:","",TS)
            HFILE=paste("/media/MasterDrive/Services/BDAQ/HPV/DPHPV-",RowkeyLatest,TS,".csv",sep="")
            HFILE2=paste("/media/MasterDrive/Services/BDAQ/DPHPV/DPHPV-",RowkeyLatest,TS,".csv",sep="")
            write.table(DPH,HFILE2,row.names=F,col.names=F,quote = F,sep=",",append = FALSE)
            file.rename(HFILE2,HFILE);
            HFILE1=paste("/media/MasterDrive/Services/BDAQ/HistoryD/DPHPV-",RowkeyLatest,TS,".csv",sep="")
            write.table(DPH,HFILE1,row.names=F,col.names=F,quote = F,sep=",",append = FALSE)
            con <- dbConnect(dbDriver("MySQL"),user=userDB,password=passDB,dbname=DBName,host=DBHost,port=3306)
            suppressWarnings(dbGetQuery(con,paste0("insert into LATESTPARAMETERVALUE (ROWKEY,MODELPARAMETERID,EQUIPMENTID,VALUE,RECORDEDTIMESTAMP,UPDATEDTIMESTAMP) values ('",DPL[,"ROWKEY"],"',",paste(unlist(DPL[,2:4]),collapse=","),
                                                   ",'",paste(unlist(DPL[,5:6]),collapse="','"),"') ON DUPLICATE KEY UPDATE VALUE=",DPL[,"VALUE"],",RECORDEDTIMESTAMP='",DPL[,"RECORDEDTIMESTAMP"],"',UPDATEDTIMESTAMP='",DPL[,"UPDATEDTIMESTAMP"],"';")))
            dbDisconnect(con);
            return(1);
          }else{return(0);}
        }
        
        CTFrame<-function(ResultSet,Condition,MPIDS,CMPIDS,MPIDSE)
        {
          DP=data.frame()
          TS=unique(sort(c(ResultSet[,"RECORDEDTIMESTAMP"],ResultSet[,"UPDATEDTIMESTAMP"])))
          prs <- with(ResultSet, paste(RECORDEDTIMESTAMP,UPDATEDTIMESTAMP,sep="."))
          t=1
          LASTV[1:length(MPIDS)]=c(0)
          #options(warn=2)
          while(t<=length(TS))
          {
            Ind=match(TS[t],ResultSet[,"UPDATEDTIMESTAMP"])
            NInd=match(TS[t+1],ResultSet[,"UPDATEDTIMESTAMP"])
            RInd=match(TS[t+1],ResultSet[,"RECORDEDTIMESTAMP"])
            MI=0
            ValueMPID=c()
            if(!is.na(Ind))
            { 
              Index = prs %in% paste(TS[t], TS[t], sep=".")
              Index1=intersect(which(ResultSet[,4]<=TS[t]),which(ResultSet[,5]>=TS[t]))
              Index=unique(c(which(Index),Index1))
              if(any(Index))
              {
                rs=ResultSet[Index,3:6]
                LI=match(sort(rs[,"MODELPARAMETERID"]),MPIDS)
                LASTV[LI]=rs[ order(rs[,1]),4]
                ValueMPID[LI]=LASTV[LI]
                MI=which(is.na(match(MPIDS,rs[,"MODELPARAMETERID"])))
              }else{MI=c(1:length(MPIDS))}
              
              if(length(MI)!=0)
              {
                if(nrow(DP)==0 && (nrow(Pointer)==0 || is.na(!match(IsDerivedResult[l,"EQUIPMENTID"],Pointer[,"EQUIPMENTID"]))))
                {
                  ValueMPID[MI]=0.0
                }else{
                  ValueMPID[MI]=LASTV[MI]
                }
              }
              a=as.data.frame(matrix(c(TS[t],TS[t],ValueMPID),ncol=2+length(MPIDS)),stringsAsFactors=F)
              colnames(a)[1:ncol(a)]=c("RECORDEDTIMESTAMP","UPDATEDTIMESTAMP",MPIDS)
              DP = rbind(DP,a)
            }else if(!is.na(NInd))
            {
              flagL=0
              Index = prs %in% paste(TS[t], TS[t+1], sep=".")
              if(any(Index))
              {
                rs=ResultSet[Index,3:6]
                LI=match(sort(rs[,"MODELPARAMETERID"]),MPIDS)
                LASTV[LI]=rs[ order(rs[,1]),4]
                ValueMPID[LI]=LASTV[LI]
                MI=which(is.na(match(MPIDS,rs[,"MODELPARAMETERID"]))) 
              }else{MI=c(1:length(MPIDS))} 
              
              if(length(MI)!=0)
              {
                if(nrow(DP)==0 && (nrow(Pointer)==0 || is.na(!match(IsDerivedResult[l,"EQUIPMENTID"],Pointer[,"EQUIPMENTID"]))))
                {
                  ValueMPID[MI]=0.0
                }else if(length(NInd)>length(which(Index))||any(ResultSet[ResultSet[,4]<=TS[t],5] >TS[t+1])||any(ResultSet[ResultSet[,4]<TS[t],5] >=TS[t+1])){  
                  if(length(which(Index))>0)
                  {Index=NInd[is.na(match(NInd,which(Index)))]
                  }else{Index=NInd} 
                  
                  if(any(ResultSet[ResultSet[,4]<=TS[t],5] >TS[t+1])){
                    Index1=intersect(which(ResultSet[,4]<=TS[t]),which(ResultSet[,5]>TS[t+1]))
                  }else{
                    Index1=intersect(which(ResultSet[,4]<TS[t]),which(ResultSet[,5]>=TS[t+1]))
                  }
                  Index=unique(c(Index,Index1))
                  a=data.frame(cbind(ResultSet[Index,3],ResultSet[Index,6]),stringsAsFactors=F)
                  LI=match(sort(ResultSet[Index,3]),MPIDS)
                  LASTV[LI]=a[order(a[,1]),2]
                  ValueMPID[LI]=LASTV[LI]
                  MI=which(is.na(match(MI,LI)))
                  if(length(MI)>0)
                  {
                    ValueMPID[MI]=LASTV[MI]
                  }
                  
                  if(flagL==0)
                  {
                    flagL=1
                    a=as.data.frame(matrix(c(TS[t],TS[t+1],ValueMPID),ncol=2+length(MPIDS)),stringsAsFactors=F)
                    colnames(a)[1:ncol(a)]=c("RECORDEDTIMESTAMP","UPDATEDTIMESTAMP",MPIDS)
                    DP = rbind(DP,a)
                  }
                  if(length(Index1)>0){ TS[t+1]=as.character(as.POSIXlt(TS[t+1])+1)
                  }else{ t=t+1 }   
                  
                }else{
                  ValueMPID[MI]=LASTV[MI]
                }
                
              }
              
              if(flagL==0)
              {
                a=as.data.frame(matrix(c(TS[t],TS[t+1],ValueMPID),ncol=2+length(MPIDS)),stringsAsFactors=F)
                colnames(a)[1:ncol(a)]=c("RECORDEDTIMESTAMP","UPDATEDTIMESTAMP",MPIDS)
                DP = rbind(DP,a)
              }
            }else if(!is.na(RInd))
            {
              Index=ResultSet[ResultSet[,4]>=TS[t],5] < TS[t+1]
              Time=as.character(as.POSIXlt(TS[t+1])-1)
              Index1=intersect(which(ResultSet[,4]<=TS[t]),which(ResultSet[,5]>=Time))
              Index=unique(c(which(Index),Index1))
              a=data.frame(cbind(ResultSet[Index,3],ResultSet[Index,6]),stringsAsFactors=F)
              LI=match(sort(ResultSet[Index,3]),MPIDS)
              LASTV[LI]=a[order(a[,1]),2]
              ValueMPID[LI]=LASTV[LI]
              MI=which(is.na(match(MPIDS,ResultSet[Index,3])))
              if(length(MI)>0)
              {
                ValueMPID[MI]=LASTV[MI]
              }
              
              a=as.data.frame(matrix(c(TS[t],Time,ValueMPID),ncol=2+length(MPIDS)),stringsAsFactors=F)
              colnames(a)[1:ncol(a)]=c("RECORDEDTIMESTAMP","UPDATEDTIMESTAMP",MPIDS)
              DP = rbind(DP,a)  
              
            }
            #cat(t, "\n")
            t=t+1
          }
          rm(ResultSet)
          DPH=data.frame()
          Expressiond=Condition
          for(E in 1:nrow(DP))
          {
            exp=Expressiond
            for(D in 1:length(CMPIDS)){
              MI=gregexpr("(?<![.\\d])\\d{4}(?![.\\d])",exp,perl=T)
              if(length(MI[[1]])>(length(CMPIDS)-D+1))
              {
                count=0;
                for(Index in 1:length(MI[[1]]))
                {
                  SMI=gregexpr("(?<![.\\d])\\d{4}(?![.\\d])",exp,perl=T)
                  SI=SMI[[1]][Index-count]
                  if(!is.na(match(CMPIDS[D],substr(exp,SI,SI+3))))
                  {
                    exp=paste(substr(exp, 1,SI-1),DP[E,D+2],substr(exp,(SI+4), nchar(exp)), sep='')
                    count=count+1
                  }
                }
                
              }else{
                #colnames(DP)[D+2]
                #str_locate("aaa12xxx", "DP[]")
                exp=gsub(colnames(DP)[D+2],DP[E,D+2],exp,fixed=T)
                #exp=paste(substr(exp, 1, MI[[1]][1]-1),DP[E,D+2],substr(exp,(MI[[1]][1]+attr(MI[[1]], "match.length")[1]), nchar(exp)), sep='')
                #exp=gsub(MPIDS[D],DP[E,D+2],exp,fixed=T)
              }
            }
            Res=eval(parse(text=paste0(exp)))
            if(isTRUE(Res))
            {
              for(MD in 1:length(MPIDSE))
              {      
                DPH=rbind(DPH,cbind(MPIDSE[MD],DP[E,MPIDSE[MD]],DP[E,1:2]))
              }
            }  
          }
          if(length(DPH)>0)
          {
            colnames(DPH)=c("MODELPARAMETERID","VALUE","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP");
            return(DPH);
          }
        }
        
        Daily<-function(ExpressionM=NULL,Fname=NULL,Shift=NULL,Condition=NULL,IsDerivedResult,Pointer,logger,LFLAG)
        {
          TFlag=0;
          OFlag=0;
          MPIDS=c();
          HMPIDS=c();
          LMPIDS=c();
          if(length(ExpressionM)>0)
          {
            rimpala.init(libs="/opt/cloudera/parcels/IMPALA/lib/impala/lib/")
            rimpala.connect("x.x.x.x","21050")          
            
            LASTV=c()
            CFlag=0;
            AFlag=0;
            con <- dbConnect(dbDriver("MySQL"),user=userDB,password=passDB,dbname=DBName,host=DBHost,port=3306)
            cm <- gregexpr("CM[0-9]*", ExpressionM, perl=TRUE)
            if(any(cm[[1]]!=-1))
            {
              CMMPIDS=sort(unique(unlist(regmatches(ExpressionM,cm))));
              CMMPIDS=gsub("CM","",CMMPIDS);
              CMMPVAL=suppressWarnings(dbGetQuery(con,paste0("SELECT MODELPARAMETERID,PARAMETERVALUE FROM COMMISSIONINGPOINT where EQUIPMENTID=",IsDerivedResult[1,"EQUIPMENTID"]," and MODELPARAMETERID IN (",paste(CMMPIDS,collapse=","),");")))
              if(nrow(CMMPVAL)>0)
              {
                for(c in 1:nrow(CMMPVAL))
                {
                  ExpressionM=gsub(paste("CM",CMMPVAL[c,1],sep=""),CMMPVAL[c,2],ExpressionM)
                }
                ExpressionM=gsub("CM","",ExpressionM);
                
              }else{
                ExpressionM=gsub("CM","HM",ExpressionM);
              }
            }
            hm <- gregexpr("HM[0-9]*", ExpressionM, perl=TRUE)
            if(any(hm[[1]]!=-1))
            {
              HMPIDS=sort(unique(unlist(regmatches(ExpressionM,hm))))
              HMPIDS=gsub("HM","",HMPIDS)
              MPIDS=c(MPIDS,HMPIDS)
            }
            lm <- gregexpr("LM[0-9]+", ExpressionM, perl=TRUE)
            if(any(lm[[1]]!=-1))
            {
              LMPIDS=sort(unique(unlist(regmatches(ExpressionM,lm))));
              LMPIDS=gsub("LM","",LMPIDS);
              MPIDS=c(MPIDS,LMPIDS);
              pm<- gregexpr("@[A-Z]*", ExpressionM, perl=TRUE);
              pm=sort(unique(unlist(regmatches(ExpressionM,pm))));
              pm=gsub("@","",pm);
            }
            lml <- gregexpr("LML[0-9]+", ExpressionM, perl=TRUE)
            if(any(lml[[1]]!=-1))
            {
              LMLMPID=sort(unique(unlist(regmatches(ExpressionM,lml))));
              LMLMPID=gsub("LML","",LMLMPID);
              MPIDS=c(MPIDS,LMLMPID);
              LMPIDS=c(LMPIDS,LMLMPID);          
            }
            MPIDSE=MPIDS;
            if(!is.null(Condition))
            {
              cm <- gregexpr("CM[0-9]*",Condition, perl=TRUE)
              if(any(cm[[1]]!=-1))
              {
                CMMPIDS=sort(unique(unlist(regmatches(Condition,cm))));
                CMMPIDS=gsub("CM","",CMMPIDS);
                CMMPVAL=suppressWarnings(dbGetQuery(con,paste0("SELECT MODELPARAMETERID,PARAMETERVALUE FROM COMMISSIONINGPOINT where EQUIPMENTID=",IsDerivedResult[1,"EQUIPMENTID"]," and MODELPARAMETERID IN (",paste(CMMPIDS,collapse=","),");")))
                if(nrow(CMMPVAL)>0)
                {
                  for(c in 1:nrow(CMMPVAL))
                  {
                    Condition=gsub(paste("CM",CMMPVAL[c,1],sep=""),CMMPVAL[c,2],Condition)
                  }
                  Condition=gsub("CM","",Condition);
                  
                }else{
                  Condition=gsub("CM","HM",Condition);
                }
                
                
              }
              cm <- gregexpr("HM[0-9]*",Condition, perl=TRUE)
              if(any(cm[[1]]!=-1))
              {
                CMPIDS=sort(unique(unlist(regmatches(Condition,cm))))
                CMPIDS=gsub("HM","",CMPIDS)
                CInd=is.na(match(CMPIDS,MPIDS))
                if(any(CInd))
                {MPIDS=c(MPIDS,CMPIDS);}        
                
              }
            }
            OffMPIDS=suppressWarnings(dbGetQuery(con,paste0("SELECT ANALOGMODELPARAMETERID FROM EQUIPMENTPARAMETER WHERE ISONSCREEN=1 AND EQUIPMENTID=",IsDerivedResult[1,"EQUIPMENTID"]," AND  ANALOGMODELPARAMETERID IN (",paste(MPIDS,collapse=","),");")))  
            if(LFLAG==0 && nrow(OffMPIDS)>0){
              Q=paste0("'",unlist(OffMPIDS),"-",IsDerivedResult[1,"EQUIPMENTID"],"'",collapse=" OR ROWKEY=")
              PossibleError <- tryCatch((df=dbGetQuery(con,paste("select * from LATESTPARAMETERVALUE where ROWKEY =",Q,sep=""))),
                                        error=function(e) e
              )
              colnames(df)=c("ROWKEY","MODELPARAMETERID","EQUIPMENTID","VALUE","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP")
              for(Oind in 1:nrow(OffMPIDS))
              {
                val=df[which(OffMPIDS[Oind]==df[,"MODELPARAMETERID"]),"VALUE"]
                ExpressionM=gsub(paste("HM",OffMPIDS[Oind],sep=""),val,ExpressionM)
                MPIDS=MPIDS[is.na(match(MPIDS,OffMPIDS[Oind]))]
              }
            }     
            if(!is.null(Shift))
            {
              SETIME=ShiftSETIME(Shift,IsDerivedResult[1,"EQUIPMENTID"]);
              if(length(SETIME)>0){START=SETIME[1];END=SETIME[2];}
            }
            PossibleError <- tryCatch({
              TST=c()
              if(nrow(Pointer)==0)
              { 
                Q=paste0("'",unlist(MPIDS),"-",IsDerivedResult[1,"EQUIPMENTID"],"'",collapse=" OR ROWKEY=")
                PossibleError <- tryCatch((df=suppressWarnings(dbGetQuery(con,paste("select ROWKEY,EQUIPMENTID,MODELPARAMETERID,RECORDEDTIMESTAMP,UPDATEDTIMESTAMP,VALUE from LATESTPARAMETERVALUE where ROWKEY =",Q,sep="")))),
                                          error=function(e) e
                ) 
                if(!inherits(PossibleError, "error") && nrow(df)==length(MPIDS))
                {             
                  colnames(df)=c("ROWKEY","EQUIPMENTID","MODELPARAMETERID","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP","VALUE")
                  #             if(!is.null(Shift))
                  #             {
                  #               Cdate=paste((Sys.Date()-1),strftime(END,"%H:%M:%S"))            
                  #             }else{                
                  #               Cdate=Sys.Date()-1;
                  #             }            
                  MAXRTS=max(df[,"UPDATEDTIMESTAMP"])
                  #if(exists("LMPIDS")&&length(LMPIDS)>0 && !is.na(match("8001",LMPIDS)))
                  if(exists("LMPIDS")&&length(LMPIDS)>0)
                  {
                    MINRTS=min(df[which(LMPIDS[1]!=df[,"MODELPARAMETERID"]),"UPDATEDTIMESTAMP"])
                  }else{
                    MINRTS=min(df[,"UPDATEDTIMESTAMP"])}  
                }else{
                  TFlag=1;rimpala.close();dbDisconnect(con);return();
                }
                if(!inherits(PossibleError, "error") && TFlag!=1)
                {
                  ResultSet=data.frame()
                  for(MID in 1:length(MPIDS))
                  {
                    RSdf=data.frame();
                    Q=paste0("'",MPIDS[MID],"-",IsDerivedResult[1,"EQUIPMENTID"],"-%'",collapse=" OR ROWKEY like ");
                    MinMPID=paste(MPIDS[MID],"-",IsDerivedResult["EQUIPMENTID"],sep="");
                    MaxMPID=paste(MPIDS[MID],"-",as.integer(IsDerivedResult["EQUIPMENTID"])+1,sep="");
                    PossibleError <- tryCatch((RSdf=rimpala.query(paste("select ROWKEY,EQUIPMENTID,MODELPARAMETERID,RECORDEDTIMESTAMP,UPDATEDTIMESTAMP,VALUE from RHISTORYPARAMETERVALUE  where (rowkey>'",MinMPID,"' and rowkey<'",MaxMPID,"') and EQUIPMENTID='",IsDerivedResult[1,"EQUIPMENTID"],"' and MODELPARAMETERID='",MPIDS[MID],"'",sep=""))),
                                              error=function(e) e)
                    if(nrow(RSdf)>0)
                    {
                      colnames(RSdf)=c("ROWKEY","EQUIPMENTID","MODELPARAMETERID","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP","VALUE");
                      ResultSet=rbind(ResultSet,RSdf);  
                    }else{TFlag=1;rimpala.close();dbDisconnect(con);return();}
                    
                  }
                  
                }
                if(!inherits(PossibleError, "error") && TFlag!=1)
                {
                  colnames(ResultSet)=c("ROWKEY","EQUIPMENTID","MODELPARAMETERID","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP","VALUE")  
                  LASTV[1:length(MPIDS)]=0                
                  ResultSet[,"RECORDEDTIMESTAMP"]=gsub("\\.0","",ResultSet[,"RECORDEDTIMESTAMP"])
                }else{TFlag=1;rimpala.close();dbDisconnect(con);return();}
                
                if(length(ResultSet)==0)
                {
                  TFlag=1;rimpala.close();dbDisconnect(con);return();
                }
                
              }else{
                FROM=Pointer[,"POINTER"]
                PossibleError <- tryCatch((FROMA=suppressWarnings(dbGetQuery(con,paste("select recordedtimestamp from LATESTPARAMETERVALUE where ROWKEY ='",ExpressionSet[i,"MODELPARAMETERID"],"-",IsDerivedResult[1,"EQUIPMENTID"],"'",sep="")))[[1]]),
                                          error=function(e) e)
                if(!inherits(PossibleError, "error"))
                {
                  Q=paste0("'",unlist(MPIDS),"-",IsDerivedResult[1,"EQUIPMENTID"],"'",collapse=" OR ROWKEY=")
                  PossibleError <- tryCatch((df=suppressWarnings(dbGetQuery(con,paste("select ROWKEY,EQUIPMENTID,MODELPARAMETERID,RECORDEDTIMESTAMP,UPDATEDTIMESTAMP,VALUE from LATESTPARAMETERVALUE where ROWKEY =",Q," and updatedtimestamp >='",FROM,"'",sep="")))),
                                            error=function(e) e
                  )
                  if(!inherits(PossibleError, "error") && nrow(df)==length(MPIDS))
                  {
                    colnames(df)=c("ROWKEY","EQUIPMENTID","MODELPARAMETERID","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP","VALUE")
                    MAXRTS=max(df[,"UPDATEDTIMESTAMP"])
                    if(exists("LMPIDS")&&length(LMPIDS)>0)
                    {
                      MINRTS=min(df[which(LMPIDS[1]!=df[,"MODELPARAMETERID"]),"UPDATEDTIMESTAMP"])
                    }else{
                      MINRTS=min(df[,"UPDATEDTIMESTAMP"])}  
                  }else{
                    TFlag=1;rimpala.close();dbDisconnect(con);return();
                  }
                  #if(!inherits(PossibleError, "error") && MINRTS>FROMA && FROM<=FROMA)
                  if(!inherits(PossibleError, "error") && TFlag!=1 && MINRTS>=FROM)
                  {
                    ResultSet=data.frame()
                    for(MID in 1:length(MPIDS))
                    {
                      RSdf=data.frame();
                      Q=paste0("'",MPIDS[MID],"-",IsDerivedResult[1,"EQUIPMENTID"],"-%'",collapse=" OR ROWKEY like ");
                      MinMPID=paste(MPIDS[MID],"-",IsDerivedResult["EQUIPMENTID"],sep="");
                      MaxMPID=paste(MPIDS[MID],"-",as.integer(IsDerivedResult["EQUIPMENTID"])+1,sep="");
                      PossibleError <- tryCatch((RSdf=rimpala.query(paste("select ROWKEY,EQUIPMENTID,MODELPARAMETERID,RECORDEDTIMESTAMP,UPDATEDTIMESTAMP,VALUE from RHISTORYPARAMETERVALUE  where (rowkey>'",MinMPID,"' and rowkey<'",MaxMPID,"') and EQUIPMENTID='",IsDerivedResult[1,"EQUIPMENTID"],"' and MODELPARAMETERID='",MPIDS[MID],"' and updatedtimestamp >='",FROM,"'",sep=""))),
                                                error=function(e) e)
                      if(nrow(RSdf)>0)
                      {
                        colnames(RSdf)=c("ROWKEY","EQUIPMENTID","MODELPARAMETERID","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP","VALUE");
                        ResultSet=rbind(ResultSet,RSdf);  
                      }else{TFlag=1;rimpala.close();dbDisconnect(con);return();}
                      
                    } 
                    if(inherits(PossibleError, "error")||!exists("ResultSet")|| length(unique(ResultSet[,3]))!=length(MPIDS))
                    {TFlag=1;rimpala.close();dbDisconnect(con);return();}
                    
                    if(!inherits(PossibleError, "error")&& TFlag!=1 && exists("ResultSet"))
                    {
                      colnames(ResultSet)=c("ROWKEY","EQUIPMENTID","MODELPARAMETERID","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP","VALUE")  
                      #                 if(length(FROMA)>0 && FROMA>FROM)
                      #                 {
                      #                   Indx=ResultSet[,"RECORDEDTIMESTAMP"]>FROMA
                      #                   #a=ResultSet[order(ResultSet[!Indx,3]),c("MODELPARAMETERID","VALUE")]
                      #                   a=ResultSet[!Indx,c(3,6)]
                      #                   a=a[order(a[,1]),]
                      #                   LASTV=a[c(match(MPIDS,a[,"MODELPARAMETERID"])),"VALUE"]
                      #                   LASTV[is.na(LASTV)]=0
                      #                   ResultSet=ResultSet[Indx,]
                      #                 }
                      
                    }else{
                      TFlag=1;rimpala.close();dbDisconnect(con);return();
                    }
                    
                    if(exists("MINRTS")){TST=gsub("\\.0","",MINRTS);}              
                  }else{
                    TFlag=1;rimpala.close();dbDisconnect(con);return();
                  }
                  if(TFlag!=1 && nrow(ResultSet)==0)
                  {
                    TFlag=1;rimpala.close();dbDisconnect(con);return();
                  }
                }else{
                  TFlag=1;rimpala.close();dbDisconnect(con);return();}
              }
              rimpala.close();
              if(TFlag!=1 && !inherits(PossibleError, "error"))
              {
                if(!is.null(Condition))
                { 
                  Condition=gsub("HM|LM","",Condition);
                  if(length(MPIDS)>1||length(CMPIDS)>1)
                  {
                    ResultSet=CTFrame(ResultSet,Condition,MPIDS,CMPIDS,MPIDSE);
                    if(nrow(ResultSet)==0){TFlag=1;rimpala.close();dbDisconnect(con);return();
                    }else{                
                      CFlag=1;
                      MPIDS=MPIDSE; 
                      Mind=min(which(as.integer(as.character(ResultSet[,"VALUE"]))>0));
                      if(length(Mind)>0 && Mind!=0){ResultSet=ResultSet[Mind:nrow(ResultSet),]}
                    }
                    
                  }else{
                    ResultSet=ResultSet[eval(parse(text=gsub(MPIDS[1],"ResultSet[,'VALUE']",Condition))),]  
                    if(nrow(ResultSet)==0){TFlag=1;return;
                    }
                  }
                  if(TFlag!=1 && is.null(Fname))
                  {
                    AFlag=1;
                    if(CFlag==1)
                    {
                      DPH=data.frame();               
                      RKH=suppressWarnings(paste(IsDerivedResult[1,"MODELPARAMETERID"],IsDerivedResult[1,"EQUIPMENTID"],ResultSet[,"RECORDEDTIMESTAMP"],sep="-"));
                      DPH=data.frame(cbind(RKH,IsDerivedResult[1,"MODELPARAMETERID"],IsDerivedResult[1,"EQUIPMENTID"],ResultSet[,"VALUE"],ResultSet[,"RECORDEDTIMESTAMP"]));
                      colnames(DPH)=c("ROWKEY","MODELPARAMETERID","EQUIPMENTID","VALUE","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP");
                      rm(ResultSet);
                      TST=DPH[nrow(DPH),"UPDATEDTIMESTAMP"]
                      
                      
                    }else{
                      DPH=data.frame();
                      ResultSet[,"MODELPARAMETERID"]=IsDerivedResult[1,"MODELPARAMETERID"];
                      DPH=ResultSet;
                      TST=DPH[nrow(DPH),"UPDATEDTIMESTAMP"];
                    }
                    
                  }
                  
                }
                
                if(length(LMPIDS)>0)
                {
                  if(exists("LMLMPID") && length(LMLMPID)>0 && pm=="ICT")
                  {
                    MIX=!is.na(match(ResultSet[,"MODELPARAMETERID"],LMLMPID))
                    MSubSet=ResultSet[MIX,];
                    MVAL=unique(MSubSet[,"VALUE"]);
                    MVALUES=paste(unlist(MVAL),collapse=",");
                    LWMPID=LMPIDS[is.na(match(LMPIDS,LMLMPID))]
                    IX=!is.na(match(ResultSet[,"MODELPARAMETERID"],LWMPID))
                    SubSet=ResultSet[IX,];
                    VAL=unique(SubSet[,"VALUE"]);
                    VALUES=paste(unlist(VAL),collapse=",");
                    ICTdf=suppressWarnings(dbGetQuery(con,paste0("select PRODUCTSERIALNUMBER,MOULDCODE,IDEALCYCLETIME from PRODUCTMACHINEMOULDMASTER where EQUIPMENTID=",IsDerivedResult[1,'EQUIPMENTID']," and MOULDCODE IN (",MVALUES,") and PRODUCTSERIALNUMBER IN (",VALUES,");")));
                    if(nrow(ICTdf)>0 && length(SubSet)>0)
                    {
                      PMDF=paste(ICTdf[,"PRODUCTSERIALNUMBER"],ICTdf[,"MOULDCODE"],sep="-");
                      LMM=1;
                      Rindex=c();
                      MVALUES=c();
                      PSVALUES=c();
                      for(LM in 1:nrow(SubSet))
                      {
                        if(SubSet[LM,"VALUE"]!=0 && LMM<=nrow(MSubSet))
                        {
                          if(SubSet[LM,"UPDATEDTIMESTAMP"]==MSubSet[LMM,"UPDATEDTIMESTAMP"]||SubSet[LM,"RECORDEDTIMESTAMP"]==MSubSet[LMM,"RECORDEDTIMESTAMP"])
                          {
                            valuesubset=ICTdf[which(paste(SubSet[LM,"VALUE"],MSubSet[LMM,"VALUE"],sep="-")==PMDF),"IDEALCYCLETIME"]
                            if(length(valuesubset)>0)
                            {
                              SubSet[LM,"VALUE"]=valuesubset;
                              #MVALUES=c(MVALUES,MSubSet[LM,"VALUE"]);
                              #PSVALUES=c(PSVALUES,SubSet[LM,"VALUE"]);
                            }else{
                              #MVALUES=c(MVALUES,MSubSet[LM,"VALUE"]);
                              #PSVALUES=c(PSVALUES,SubSet[LM,"VALUE"]);
                              index=rownames(SubSet[LM,]);
                              Rindex=c(Rindex,index);
                            }
                          }else{
                            Ind=which(SubSet[LM,"UPDATEDTIMESTAMP"]==MSubSet[,"UPDATEDTIMESTAMP"])
                            if(length(Ind)>0)
                            {
                              LMM=min(Ind[length(Ind)]);
                              valueict=ICTdf[which(paste(SubSet[LM,"VALUE"],MSubSet[LMM,"VALUE"],sep="-")==PMDF),"IDEALCYCLETIME"];
                              if(length(valueict)>0){SubSet[LM,"VALUE"]=valueict};
                            }else{
                              Ind=which(SubSet[LM,"RECORDEDTIMESTAMP"]==MSubSet[,"RECORDEDTIMESTAMP"])
                              if(length(Ind)>0)
                              {
                                #LMM=Ind[length(Ind)]
                                LMM=min(Ind[[MSubSet[Ind,"VALUE"]!=0]]);
                                valueict=ICTdf[which(paste(SubSet[LM,"VALUE"],MSubSet[LMM,"VALUE"],sep="-")==PMDF),"IDEALCYCLETIME"];
                                if(length(valueict)>0){SubSet[LM,"VALUE"]=valueict};
                              }else{
                                Ind=c();CIndx=c();DIndx=c();BIndx=c();MinTs=SubSet[LM,"RECORDEDTIMESTAMP"];MaxTs=SubSet[LM,"UPDATEDTIMESTAMP"];
                                CIndx=MSubSet[,"RECORDEDTIMESTAMP"]<MinTs & MSubSet[,"UPDATEDTIMESTAMP"]>=MaxTs
                                DIndx=MSubSet[,"UPDATEDTIMESTAMP"]>=MinTs & MSubSet[,"UPDATEDTIMESTAMP"]<=MaxTs
                                EIndx=MSubSet[,"RECORDEDTIMESTAMP"]>=MinTs & MSubSet[,"UPDATEDTIMESTAMP"]>=MaxTs & MSubSet[,"RECORDEDTIMESTAMP"]<=MaxTs
                                BIndx=MSubSet[,"RECORDEDTIMESTAMP"]==MSubSet[,"UPDATEDTIMESTAMP"] & MSubSet[,"RECORDEDTIMESTAMP"]<=MaxTs & MSubSet[,"RECORDEDTIMESTAMP"]>=MinTs
                                CIndx=CIndx|DIndx|EIndx|BIndx;
                                if(length(CIndx)>0 && any(CIndx))
                                { 
                                  LMM=which(CIndx)[MSubSet[which(CIndx),"VALUE"]!=0][1];
                                  valueict=ICTdf[which(paste(SubSet[LM,"VALUE"],MSubSet[LMM,"VALUE"],sep="-")==PMDF),"IDEALCYCLETIME"];
                                  if(length(valueict)>0){SubSet[LM,"VALUE"]=valueict};
                                }else{
                                  VAL=ICTdf[which(paste(SubSet[LM,"VALUE"],MSubSet[nrow(MSubSet),"VALUE"],sep="-")==PMDF),"IDEALCYCLETIME"];
                                  if(length(VAL)>0){SubSet[LM,"VALUE"]=ICTdf[which(paste(SubSet[LM,"VALUE"],MSubSet[nrow(MSubSet),"VALUE"],sep="-")==PMDF),"IDEALCYCLETIME"];
                                  }else{index=rownames(SubSet[LM,]);
                                  Rindex=c(Rindex,index);
                                  }
                                }
                              }
                            }
                          }
                          LMM=LMM+1;
                        }else{LMM=LMM+1;}
                        if(LMM>nrow(MSubSet))
                        {
                          LMM=LMM-1;
                        }
                      }
                      ResultSet[IX,]=SubSet;
                      NNZVAL=unique(SubSet[,"VALUE"]);
                      NZVAL=VAL[VAL!=0];
                      NZVAL=NZVAL[!is.na(match(NZVAL,NNZVAL))];
                      #if(nrow(ICTdf)!=length(NZVAL) && nrow(ICTdf)<length(NZVAL))
                      if(length(NZVAL)>0)
                      {
                        NZVAL=NZVAL[is.na(match(NZVAL,ICTdf[,"IDEALCYCLETIME"]))];
                        if(length(NZVAL)>0)
                        {
                          index=rownames(SubSet[!is.na(match(SubSet[,"VALUE"],NZVAL)),]);
                          Rindex=c(Rindex,index);
                        }
                      }
                      #                     NZVAL=MVAL[MVAL!=0];
                      #                     NZVAL=NZVAL[is.na(match(NZVAL,MVALUES))]
                      #                     if(nrow(ICTdf)!=length(NZVAL) && nrow(ICTdf)<length(NZVAL))
                      #                     {
                      #                       NZVAL=NZVAL[NZVAL!=ICTdf[,"MOULDCODE"]];
                      #                       if(length(NZVAL)>0)
                      #                       {
                      #                         index=rownames(MSubSet[!is.na(match(MSubSet[,"VALUE"],NZVAL)),]);
                      #                         Rindex=c(Rindex,index);
                      #                       }
                      #                     }
                      if(length(Rindex)>0){ResultSet=ResultSet[-as.integer(Rindex),];}
                      
                      
                    }else{
                      index=rownames(SubSet[SubSet[,"VALUE"]!=0,]);
                      index=c(index,rownames(MSubSet[MSubSet[,"VALUE"]!=0,]));
                      ResultSet=ResultSet[-as.integer(index),]
                    }
                    
                    
                  }else{
                    IX=!is.na(match(ResultSet[,"MODELPARAMETERID"],LMPIDS))
                    SubSet=ResultSet[IX,];
                    VAL=unique(SubSet[,"VALUE"]);
                    VALUES=paste(unlist(VAL),collapse=",");
                    if(pm=="STW")
                    {
                      LMdf=suppressWarnings(dbGetQuery(con,paste0("select SERIALNUMBER,STANDARDWEIGHT from PRODUCTMASTER where SERIALNUMBER IN (",VALUES,");")));
                    }
                    if(nrow(LMdf)>0 && length(SubSet)>0)
                    {
                      colnames(LMdf)=c("CODE","VALUE")
                      for(dfv in 1:nrow(LMdf))
                      {
                        SubSet[which(SubSet[,"VALUE"]==LMdf[dfv,"CODE"]),"VALUE"]=LMdf[dfv,"VALUE"];
                      }
                      ResultSet[IX,]=SubSet;
                      NZVAL=VAL[VAL!=0];
                      NZVAL=NZVAL[is.na(match(NZVAL,LMdf[,"CODE"]))];
                      if(length(NZVAL)>0)
                      {
                        index=rownames(SubSet[!is.na(match(SubSet[,"VALUE"],NZVAL)),]);
                        ResultSet=ResultSet[-as.integer(index),]
                      }
                      
                    }else{
                      index=rownames(SubSet[SubSet[,"VALUE"]!=0,]);
                      if(length(index)>0){ResultSet=ResultSet[-as.integer(index),]}
                      
                    }
                    
                  }
                  
                }
                
                
                if(AFlag==0 && TFlag!=1)
                {
                  if(!is.null(Shift)){              
                    if(exists("FROM")){MinDate=format(as.POSIXct(FROM),"%Y-%m-%d");
                    }else{MinDate=format(as.POSIXct(min(ResultSet[,"RECORDEDTIMESTAMP"])),"%Y-%m-%d")}
                    MaxDate=as.Date(format(as.POSIXct(max(ResultSet[,"UPDATEDTIMESTAMP"])),"%Y-%m-%d"))+1
                    if(max(ResultSet[,"UPDATEDTIMESTAMP"])<paste((as.Date(MaxDate)-1),format(as.POSIXlt(START),"%H:%M:%S")))
                    {
                      MaxDate=as.Date(MaxDate)-1
                    }                
                  }
                  for(M in 1:length(MPIDS))
                  {
                    RS=ResultSet[which(ResultSet[,"MODELPARAMETERID"]==MPIDS[M]),]
                    if(nrow(RS)>0 && TFlag!=1)
                    {
                      if(!is.null(Shift))
                      {
                        Shr=format(as.POSIXlt(START),"%H");
                        Ehr=format(as.POSIXlt(END),"%H");
                        STARTTIME=format(as.POSIXlt(START),"%H:%M:%S");
                        ENDTIME=format(as.POSIXlt(END),"%H:%M:%S");
                        MinTs=paste(seq(as.Date(MinDate), as.Date(MaxDate), "days"),STARTTIME)
                        MaxTs=paste(seq(as.Date(MinDate), as.Date(MaxDate), "days"),ENDTIME)
                        FIndx=sample(c(F),nrow(RS), replace = TRUE);
                        CIndx=c();
                        DIndx=c();
                        EIndx=c();
                        BIndx=c();
                        RS1=data.frame();
                        RTSV=c();
                        #               if(Shift==4 && Shr==Ehr)
                        #               {RS$RECORDEDTIMESTAMP=as.POSIXlt(RS$RECORDEDTIMESTAMP)-as.integer(Shr)*60*59;
                        #                 ENDTIME="00:00:00";
                        #                 DI=2;
                        #               }
                        #RS=RS[as.integer(format(as.POSIXct(RS[,"RECORDEDTIMESTAMP"]),"%H")) %in%  c(Shr:as.integer(Ehr)-1),]
                        if(ENDTIME>STARTTIME)
                        {
                          #                   RS=RS[as.integer(format(as.POSIXct(RS[,"UPDATEDTIMESTAMP"]),"%H")) %in%  c(Shr:as.integer(Ehr)),]
                          #                   RTRS=format(as.POSIXct(RS[,"UPDATEDTIMESTAMP"]),"%H:%M:%S")
                          #                   TDS=difftime(as.POSIXct(ENDTIME,format="%H:%M:%S"),as.POSIXct(STARTTIME,format="%H:%M:%S"),unit="sec")
                          #                   RTRS=difftime(as.POSIXct(ENDTIME,format="%H:%M:%S"),as.POSIXct(RTRS,format="%H:%M:%S"),unit="sec")
                          #                   RS=RS[(RTRS<=TDS & RTRS >=0),]
                          #for(i in 1:(length(MaxTs)-1))
                          for(i in 1:length(MaxTs))
                          {
                            CIndx=RS[,"RECORDEDTIMESTAMP"]<MinTs[i] & RS[,"UPDATEDTIMESTAMP"]>=MaxTs[i]
                            DIndx=RS[,"UPDATEDTIMESTAMP"]>=MinTs[i] & RS[,"UPDATEDTIMESTAMP"]<=MaxTs[i]
                            EIndx=RS[,"RECORDEDTIMESTAMP"]>=MinTs[i] & RS[,"UPDATEDTIMESTAMP"]>=MaxTs[i] & RS[,"RECORDEDTIMESTAMP"]<=MaxTs[i]
                            BIndx=RS[,"RECORDEDTIMESTAMP"]==RS[,"UPDATEDTIMESTAMP"] & RS[,"RECORDEDTIMESTAMP"]<=MaxTs[i] & RS[,"RECORDEDTIMESTAMP"]>=MinTs[i]
                            CIndx=CIndx|DIndx|EIndx|BIndx;
                            if(any(CIndx))
                            { 
                              RS1=rbind(RS1,cbind(RS[CIndx,],MinTs[i]));                    
                              
                            }
                          }
                          #FIndx=CIndx|FIndx
                          
                        }else{
                          for(i in 1:(length(MaxTs)-1))
                          {
                            CIndx=RS[,"RECORDEDTIMESTAMP"]<MinTs[i] & RS[,"UPDATEDTIMESTAMP"]>=MaxTs[i+1]
                            DIndx=RS[,"UPDATEDTIMESTAMP"]>=MinTs[i] & RS[,"UPDATEDTIMESTAMP"]<=MaxTs[i+1]
                            EIndx=RS[,"RECORDEDTIMESTAMP"]>=MinTs[i] & RS[,"UPDATEDTIMESTAMP"]>=MaxTs[i+1] & RS[,"RECORDEDTIMESTAMP"]<=MaxTs[i+1]
                            BIndx=RS[,"RECORDEDTIMESTAMP"]==RS[,"UPDATEDTIMESTAMP"] & RS[,"RECORDEDTIMESTAMP"]<=MaxTs[i] & RS[,"RECORDEDTIMESTAMP"]>=MinTs[i]
                            CIndx=CIndx|DIndx|EIndx|BIndx;
                            if(any(CIndx))
                            { 
                              RS1=rbind(RS1,cbind(RS[CIndx,],MinTs[i]));                  
                              
                            }
                            
                            #FIndx=CIndx|FIndx
                          }
                        }
                        if(length(RS1)>0)
                        {
                          if(CFlag!=1)
                          {
                            colnames(RS1)=c("ROWKEY","EQUIPMENTID","MODELPARAMETERID","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP","VALUE","RTS")  
                          }else{
                            colnames(RS1)=c("MODELPARAMETERID","VALUE","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP","RTS")
                          }
                          RS=RS1;
                        }else{ TFlag=1;rimpala.close();dbDisconnect(con);return();}
                        
                      }else{
                        RS=data.frame(cbind(RS,RS$UPDATEDTIMESTAMP))
                        colnames(RS)[ncol(RS)]="RTS";
                        ENDTIME="00:00:00";
                        DI=2;
                      }
                      
                      if(!is.null(Shift)){if(Shift==4 && Shr==Ehr){DI=2;}else{DI=1;}}
                      
                      Days=strftime(RS$RTS,"%Y-%m-%d")
                      if(is.null(Fname) && CFlag!=1){Fname="mean"};
                      if(Fname %in% c("WeightedAverage","TimeTotalized","Cumulative")) 
                      {
                        NoOfDays=unique(Days)
                        #                   if(length(NoOfDays)>=DI)
                        #                   {   
                        #                     #Dy=NoOfDays[2:(length(NoOfDays)-1)]
                        #                     Dy=NoOfDays[DI:length(NoOfDays)]
                        #                     MaxDayT=Dy[is.na(match(paste(Dy,ENDTIME),RS$RECORDEDTIMESTAMP))]
                        #                     if(length(MaxDayT)>0)
                        #                     {
                        #                       RST=data.frame()
                        #                       for(i in 1:length(MaxDayT))
                        #                       {
                        #                         Ts=paste(MaxDayT[i],ENDTIME)
                        #                         Days[length(Days)+1]=MaxDayT[i]
                        #                         if(i!=length(MaxDayT))
                        #                         {df=RS[max(which(MaxDayT[i+1]>as.character(RS$RECORDEDTIMESTAMP))),]
                        #                         }else{
                        #                           df=RS[nrow(RS),]
                        #                         }
                        #                         cf=cbind(df[1,1:3],Ts,Ts,df[1,6:7])
                        #                         colnames(cf)=colnames(RS)
                        #                         RST=rbind(RST,cf)
                        #                       }
                        #                       RS=rbind(RS,RST)
                        #                     }
                        #                     #RS=RS[order(RS[,4]),]  
                        #                   }
                        if(Fname!="Cumulative")
                        {
                          df=data.frame();
                          Res=c();
                          for(i in 1:length(NoOfDays))
                          {
                            df=RS[which(Days==NoOfDays[i]),]
                            Res[i]=eval(parse(text=paste0(Fname,"(df)"))) 
                          }
                          aggdf=cbind(NoOfDays,Res);
                        }else{aggdf=aggregate(RS$VALUE,by=list(Days),Fname)}
                        
                      }else{ 
                        if(Fname=='TTime')
                        {
                          diff=as.POSIXct(RS$UPDATEDTIMESTAMP,format='%Y-%m-%d %H:%M:%S')-as.POSIXct(RS$RECORDEDTIMESTAMP,format='%Y-%m-%d %H:%M:%S')+1
                          Fname='sum';
                          RS$VALUE=diff;
                        }
                        aggdf=aggregate(RS$VALUE,by=list(Days),Fname)
                      }                
                      colnames(aggdf)[1:2]=c("Date",MPIDS[M])
                      if(M>1)
                      {
                        df=merge(df, aggdf, by=c("Date"),all.x = TRUE)
                      }else
                      {
                        df=aggdf
                      }            
                    }else{TFlag=1;rimpala.close();dbDisconnect(con);return();}
                  }
                  rm(ResultSet)
                  if((ncol(df)-1)==length(MPIDS) && TFlag!=1)
                  {
                    Res=c()
                    DPH=data.frame()
                    Index=complete.cases(df)
                    if(length(Index)>1 && any(Index))
                    {
                      df=df[Index,];
                    }
                    if(nrow(df)>0){Res=ExEVALHM(df,ExpressionM,MPIDS);}
                    if(nrow(df)>0 && length(Res)>0)
                    {
                      if(!is.null(Shift))
                      {
                        #Ts=paste(df[,"Date"],strftime(START,"%H:%M:%S"))
                        #TST=paste(df[nrow(df),"Date"],strftime(END,"%H:%M:%S"))
                        if(ENDTIME<=STARTTIME)
                        {
                          #Dates=as.Date(df[,"Date"])+1
                          #Ts=as.character(as.POSIXlt(paste(Dates,ENDTIME))-1)
                          Ts=as.character(as.POSIXlt(paste(as.Date(df[,"Date"]),STARTTIME))+3660)
                        }else{
                          #Ts=as.character(as.POSIXlt(paste(df[,"Date"],strftime(END,"%H:%M:%S")))-1)
                          Ts=as.character(as.POSIXlt(paste(df[,"Date"],strftime(START,"%H:%M:%S")))+3660)
                        }
                        TST=paste(df[nrow(df),"Date"],strftime(START,"%H:%M:%S"))
                        
                      }else{ 
                        Ts=paste(df[,"Date"],"00:00:00")
                        TST=paste(df[nrow(df),"Date"],"00:00:00")  
                      }
                      #rm(df);
                      RKH=suppressWarnings(paste(IsDerivedResult[1,"MODELPARAMETERID"],IsDerivedResult[1,"EQUIPMENTID"],Ts,sep="-"));
                      DPH=data.frame(cbind(RKH,IsDerivedResult[1,"MODELPARAMETERID"],IsDerivedResult[1,"EQUIPMENTID"],Res,Ts,Ts),stringsAsFactors=FALSE);  
                    }else{TFlag=1;rimpala.close();dbDisconnect(con);return();}
                  }else{TFlag=1;rimpala.close();dbDisconnect(con);return();}
                }
              }
            },warning = function(war){ warn(logger,paste(ExpressionM,"for",IsDerivedResult[1,"MODELPARAMETERID"],"&",IsDerivedResult[1,"EQUIPMENTID"],war,sep=" "));war;}, 
            error = function(err) { error(logger,paste(ExpressionM,"for",IsDerivedResult[1,"MODELPARAMETERID"],"&",IsDerivedResult[1,"EQUIPMENTID"],err,sep=" "));err; })
            rimpala.close();
            if(TFlag!=1 && !inherits(PossibleError, "error") && !inherits(PossibleError, "warn") && nrow(DPH)>0)  
            {
              if(is.null(TST)){TST=paste(TST,"00:00:00")}        
              Flag=WriteF(DPH,paste(IsDerivedResult[1,"MODELPARAMETERID"],IsDerivedResult[1,"EQUIPMENTID"],sep="-"))
              if(Flag==1){suppressWarnings(dbGetQuery(con,paste0("insert into POINTER (MODELPARAMETERID,EQUIPMENTID,POINTER) values (",IsDerivedResult[1,"MODELPARAMETERID"],",",IsDerivedResult[1,"EQUIPMENTID"],",'",TST,"') ON DUPLICATE KEY UPDATE POINTER='",TST,"';")))}
            }
            dbDisconnect(con) 
          } 
        }
        
        Timely<-function(ExpressionM=NULL,Fname=NULL,time=NULL,Condition=NULL,TO=NULL,FROM=NULL,IsDerivedResult,Pointer,logger,LFLAG)
        {
          TFlag=0;
          OFlag=0;
          DFlag=0;
          CFlag=0;
          TST=c();
          MPIDS=c();
          PossibleError <- tryCatch({
            if(!is.null(time) && time>=60)
            {
              hr=time/60;
            }
            SETIME=ShiftSETIME(1,IsDerivedResult[1,"EQUIPMENTID"]);
            if(length(SETIME)>0)
            {
              mins=as.numeric(format(as.POSIXct(SETIME[1], format="%Y-%m-%d %H:%M"), format="%M"))*60
              
            }else{mins=0;}
            rimpala.init(libs="/opt/cloudera/parcels/IMPALA/lib/impala/lib/")
            rimpala.connect("x.x.x.x","21050")
            con <- dbConnect(dbDriver("MySQL"),user=userDB,password=passDB,dbname=DBName,host=DBHost,port=3306)
            if((is.null(ExpressionM) || nchar(gsub("\\s", "", ExpressionM))<1) && nchar(gsub("\\s", "",Condition))>0)
            {
              ExpressionM=Condition;        
              LASTV=c()
              cm <- gregexpr("CM[0-9]*", ExpressionM, perl=TRUE)
              if(any(cm[[1]]!=-1))
              {
                CMMPIDS=sort(unique(unlist(regmatches(ExpressionM,cm))));
                CMMPIDS=gsub("CM","",CMMPIDS);
                CMMPVAL=suppressWarnings(dbGetQuery(con,paste0("SELECT MODELPARAMETERID,PARAMETERVALUE FROM COMMISSIONINGPOINT where EQUIPMENTID=",IsDerivedResult[1,"EQUIPMENTID"]," and MODELPARAMETERID IN (",paste(CMMPIDS,collapse=","),");")))
                if(nrow(CMMPVAL)>0)
                {
                  for(c in 1:nrow(CMMPVAL))
                  {
                    ExpressionM=gsub(paste("CM",CMMPVAL[c,1],sep=""),CMMPVAL[c,2],ExpressionM)
                  }
                  ExpressionM=gsub("CM","",ExpressionM);
                  
                }else{
                  ExpressionM=gsub("CM","HM",ExpressionM);
                }
              }
              ExpressionMM=ExpressionM
              hm <- gregexpr("HM[0-9]*",ExpressionM, perl=TRUE)
              if(any(hm[[1]]!=-1))
              {
                HMPIDS=sort(unique(unlist(regmatches(ExpressionM,hm))))
                HMPIDS=gsub("HM","",HMPIDS)
                MPIDS=c(MPIDS,HMPIDS)
              }
              Condition=gsub("HM|CM","",Condition)
              ExpressionM=gsub("HM|CM","",ExpressionM)
              DFlag=1;
              
              
            }else{
              LASTV=c()
              cm <- gregexpr("CM[0-9]*", ExpressionM, perl=TRUE)
              if(any(cm[[1]]!=-1))
              {
                CMMPIDS=sort(unique(unlist(regmatches(ExpressionM,cm))));
                CMMPIDS=gsub("CM","",CMMPIDS);
                CMMPVAL=suppressWarnings(dbGetQuery(con,paste0("SELECT MODELPARAMETERID,PARAMETERVALUE FROM COMMISSIONINGPOINT where EQUIPMENTID=",IsDerivedResult[1,"EQUIPMENTID"]," and MODELPARAMETERID IN (",paste(CMMPIDS,collapse=","),");")))
                if(nrow(CMMPVAL)>0)
                {
                  for(c in 1:nrow(CMMPVAL))
                  {
                    ExpressionM=gsub(paste("CM",CMMPVAL[c,1],sep=""),CMMPVAL[c,2],ExpressionM)
                  }
                  ExpressionM=gsub("CM","",ExpressionM);
                  
                }else{
                  ExpressionM=gsub("CM","HM",ExpressionM);
                }
                
                
              }
              ExpressionMM=ExpressionM
              hm <- gregexpr("HM[0-9]*", ExpressionM, perl=TRUE)
              if(any(hm[[1]]!=-1))
              {
                HMPIDS=sort(unique(unlist(regmatches(ExpressionM,hm))))
                HMPIDS=gsub("HM","",HMPIDS)
                MPIDS=c(MPIDS,HMPIDS)
              }
              lm <- gregexpr("LM[0-9]+", ExpressionM, perl=TRUE)
              if(any(lm[[1]]!=-1))
              {
                LMPIDS=sort(unique(unlist(regmatches(ExpressionM,lm))));
                LMPIDS=gsub("LM","",LMPIDS);
                MPIDS=c(MPIDS,LMPIDS);
                pm<- gregexpr("@[A-Z]*", ExpressionM, perl=TRUE);
                pm=sort(unique(unlist(regmatches(ExpressionM,pm))));
                pm=gsub("@","",pm);
              }
              lml <- gregexpr("LML[0-9]+", ExpressionM, perl=TRUE)
              if(any(lml[[1]]!=-1))
              {
                LMLMPID=sort(unique(unlist(regmatches(ExpressionM,lml))));
                LMLMPID=gsub("LML","",LMLMPID);
                MPIDS=c(MPIDS,LMLMPID);
                LMPIDS=c(LMPIDS,LMLMPID);          
              }
              if(!is.null(Condition))
              {
                cm <- gregexpr("CM[0-9]*",Condition, perl=TRUE)
                if(any(cm[[1]]!=-1))
                {
                  CMMPIDS=sort(unique(unlist(regmatches(Condition,cm))));
                  CMMPIDS=gsub("CM","",CMMPIDS);
                  CMMPVAL=suppressWarnings(dbGetQuery(con,paste0("SELECT MODELPARAMETERID,PARAMETERVALUE FROM COMMISSIONINGPOINT where EQUIPMENTID=",IsDerivedResult[1,"EQUIPMENTID"]," and MODELPARAMETERID IN (",paste(CMMPIDS,collapse=","),");")))
                  if(nrow(CMMPVAL)>0)
                  {
                    for(c in 1:nrow(CMMPVAL))
                    {
                      Condition=gsub(paste("CM",CMMPVAL[c,1],sep=""),CMMPVAL[c,2],Condition)
                    }
                    Condition=gsub("CM","",Condition);
                    
                  }else{
                    Condition=gsub("CM","HM",Condition);
                  }
                  
                  
                }
                hm <- gregexpr("HM[0-9]*", Condition, perl=TRUE)
                if(any(hm[[1]]!=-1))
                {
                  CMPIDS=sort(unique(unlist(regmatches(Condition,hm))))
                  CMPIDS=gsub("HM","",CMPIDS)
                  Condition=gsub("HM|CM","",Condition)
                  if(is.na(match(CMPIDS,MPIDS)))
                  {
                    CFlag=1;
                  }
                }
              }
              ExpressionM=gsub("HM|LML|CM|LM|@[A-Z]*","",ExpressionM)
            }
            if(nchar(gsub("\\s", "", ExpressionM))>0)
            {
              OffMPIDS=suppressWarnings(dbGetQuery(con,paste0("SELECT ANALOGMODELPARAMETERID FROM EQUIPMENTPARAMETER WHERE ISONSCREEN=1 AND EQUIPMENTID=",IsDerivedResult[1,"EQUIPMENTID"]," AND  ANALOGMODELPARAMETERID IN (",paste(MPIDS,collapse=","),");")))  
              if(LFLAG==0 && nrow(OffMPIDS)>0){
                Q=paste0("'",unlist(OffMPIDS),"-",IsDerivedResult[1,"EQUIPMENTID"],"'",collapse=" OR ROWKEY=")
                PossibleError <- tryCatch((df=dbGetQuery(con,paste("select * from LATESTPARAMETERVALUE where ROWKEY =",Q,sep=""))),
                                          error=function(e) e
                )
                colnames(df)=c("ROWKEY","MODELPARAMETERID","EQUIPMENTID","VALUE","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP")
                for(Oind in 1:nrow(OffMPIDS))
                {
                  val=df[which(OffMPIDS[Oind]==df[,"MODELPARAMETERID"]),"VALUE"]
                  ExpressionM=gsub(paste("HM",OffMPIDS[Oind],sep=""),val,ExpressionM)
                  MPIDS=MPIDS[is.na(match(MPIDS,OffMPIDS[Oind]))]
                }
              }   
              if(!is.null(TO) && !is.null(FROM))
              {
                hm <- gregexpr("HM[0-9]*",TO, perl=TRUE)
                if(any(hm[[1]]!=-1))
                {
                  SMPID=sort(unique(unlist(regmatches(TO,hm))))
                  SMPID=gsub("HM","",SMPID)
                  TO=SMPID
                }
                hm <- gregexpr("HM[0-9]*",FROM, perl=TRUE)
                if(any(hm[[1]]!=-1))
                {
                  EMPID=sort(unique(unlist(regmatches(FROM,hm))))
                  EMPID=gsub("HM","",EMPID)
                  FROM=EMPID
                }
                MPIDS=c(MPIDS,SMPID,EMPID)
              }
              if(CFlag==1){MPIDS=c(MPIDS,CMPIDS);}
              info(logger,paste(IsDerivedResult[1,"MODELPARAMETERID"],"-",IsDerivedResult[1,"EQUIPMENTID"],"data fetching about to start",sep=" "))
              if(nrow(Pointer)==0)
              { 
                Q=paste0("'",unlist(MPIDS),"-",IsDerivedResult[1,"EQUIPMENTID"],"'",collapse=" OR ROWKEY=")
                PossibleError <- tryCatch((df=dbGetQuery(con,paste("select ROWKEY,EQUIPMENTID,MODELPARAMETERID,RECORDEDTIMESTAMP,UPDATEDTIMESTAMP,VALUE from LATESTPARAMETERVALUE where ROWKEY =",Q,sep=""))),
                                          error=function(e) e) 
                if(!inherits(PossibleError, "error") && nrow(df)==length(MPIDS))
                {              
                  MINRTS=min(df[,"UPDATEDTIMESTAMP"])
                  if(!is.null(time))
                  {
                    if(exists("hr"))
                    {
                      mtg=as.integer(strftime(MINRTS,"%M"))-mins/60
                      if(mtg>0)
                      {
                        MINRTS=(floor_date(as.POSIXlt(MINRTS), 'hour')- (as.integer(strftime(MINRTS,"%H"))%%hr)*3600)+mins                    
                      }else{
                        MINRTS=(floor_date(as.POSIXlt(MINRTS), 'hour')-(as.integer(strftime(MINRTS,"%H"))%%hr)*3600)-mins    
                      }
                      
                    }else{
                      MINRTS=floor_date(as.POSIXlt(MINRTS), 'min')-(as.integer(strftime(MINRTS,"%M"))%%time)*60
                    }
                  }
                  if(!is.null(TO) && !is.null(FROM))
                  {
                    MINRTS=min(df[is.na(match(df[,"MODELPARAMETERID"],TO)),"UPDATEDTIMESTAMP"])
                    if(df[!is.na(match(df[,"MODELPARAMETERID"],FROM)),"UPDATEDTIMESTAMP"]!=MINRTS)
                    {
                      TFlag=1;
                    }else{
                      MINRTS=as.character(as.POSIXlt(MINRTS)+1)
                    }
                  }
                  ResultSet=data.frame()
                  for(MID in 1:length(MPIDS))
                  {
                    RSdf=data.frame();
                    Q=paste0("'",MPIDS[MID],"-",IsDerivedResult[1,"EQUIPMENTID"],"-%'",collapse=" OR ROWKEY like ");
                    MinMPID=paste(MPIDS[MID],"-",IsDerivedResult[1,"EQUIPMENTID"],sep="");
                    MaxMPID=paste(MPIDS[MID],"-",as.integer(IsDerivedResult[1,"EQUIPMENTID"])+1,sep="");
                    PossibleError <- tryCatch((RSdf=rimpala.query(paste("select ROWKEY,EQUIPMENTID,MODELPARAMETERID,RECORDEDTIMESTAMP,UPDATEDTIMESTAMP,VALUE from RHISTORYPARAMETERVALUE  where (rowkey>'",MinMPID,"' and rowkey<'",MaxMPID,"') and EQUIPMENTID='",IsDerivedResult[1,"EQUIPMENTID"],"' and MODELPARAMETERID='",MPIDS[MID],"'",sep=""))),
                                              error=function(e) e)
                    if(nrow(RSdf)>0)
                    {
                      colnames(RSdf)=c("ROWKEY","EQUIPMENTID","MODELPARAMETERID","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP","VALUE");
                      ResultSet=rbind(ResultSet,RSdf);  
                    }else{TFlag=1;rimpala.close();dbDisconnect(con);return();}
                    
                  }
                  if(!inherits(PossibleError, "error") && length(ResultSet)!=0)
                  {
                    colnames(ResultSet)=c("ROWKEY","EQUIPMENTID","MODELPARAMETERID","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP","VALUE")  
                    if(LFLAG==1){LASTV[1:length(MPIDS)]=0;}
                    ResultSet[,"UPDATEDTIMESTAMP"]=gsub("\\.0","",ResultSet[,"UPDATEDTIMESTAMP"])
                  }else{TFlag=1;rimpala.close();dbDisconnect(con);return();}
                  TST=gsub("\\.0","",MINRTS)
                  
                }else{TFlag=1;rimpala.close();dbDisconnect(con);return();}
                
                
                
                
                
              }else{
                FROMP=Pointer[,"POINTER"]
                Q=paste0("'",unlist(MPIDS),"-",IsDerivedResult[1,"EQUIPMENTID"],"'",collapse=" OR ROWKEY=")
                PossibleError <- tryCatch((df=dbGetQuery(con,paste("select ROWKEY,EQUIPMENTID,MODELPARAMETERID,RECORDEDTIMESTAMP,UPDATEDTIMESTAMP,VALUE from LATESTPARAMETERVALUE where ROWKEY =",Q," and updatedtimestamp >'",FROMP,"'",sep=""))),
                                          error=function(e) e
                )
                if(!inherits(PossibleError, "error") && nrow(df)==length(MPIDS))
                {
                  colnames(df)=c("ROWKEY","EQUIPMENTID","MODELPARAMETERID","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP","VALUE")
                  MAXRTS=max(df[,"UPDATEDTIMESTAMP"])
                  MINRTS=min(df[,"UPDATEDTIMESTAMP"])
                  if(!is.null(time))
                  {
                    if(exists("hr")){
                      mtg=as.integer(strftime(MINRTS,"%M"))-mins/60
                      if(mtg>0)
                      {
                        MINRTS=(floor_date(as.POSIXlt(MINRTS), 'hour')- (as.integer(strftime(MINRTS,"%H"))%%hr)*3600)+mins                    
                        
                      }else{
                        MINRTS=(floor_date(as.POSIXlt(MINRTS), 'hour')-(as.integer(strftime(MINRTS,"%H"))%%hr)*3600)-mins    
                      }
                      TST=MINRTS
                      
                    }else{               
                      MINRTS=as.POSIXlt(MINRTS)-(as.integer(strftime(MINRTS,"%M"))%%time)*60
                      MINRTS=as.POSIXlt(MINRTS)-as.integer(strftime(MINRTS,"%S"))
                    }
                  }
                  if(!is.null(TO) && !is.null(FROM))
                  { MINRTS=min(df[is.na(match(df[,"MODELPARAMETERID"],TO)),"UPDATEDTIMESTAMP"])
                  Index=!is.na(match(df[,"MODELPARAMETERID"],FROM));
                  if(any(Index) && df[Index,"UPDATEDTIMESTAMP"]!=MINRTS)
                  {
                    TFlag=1;
                  }else{
                    MINRTS=as.character(as.POSIXlt(MINRTS)+1)
                  }
                  }
                  if(LFLAG==0 && nrow(df)!=length(MPIDS))
                  {
                    TFlag=1;rimpala.close();dbDisconnect(con);return();
                    
                  }else{ 
                    PossibleError <- tryCatch((FROMA=dbGetQuery(con,paste("select RECORDEDTIMESTAMP from LATESTPARAMETERVALUE where ROWKEY ='",IsDerivedResult[1,"MODELPARAMETERID"],"-",IsDerivedResult[1,"EQUIPMENTID"],"'",sep=""))[[1]]),
                                              error=function(e) e)          
                    #if(!inherits(PossibleError, "error") && length(FROMA)>0 && MAXRTS>FROM && (difftime(MINRTS,FROM,units="mins")>=time||exists("hr") && difftime(MINRTS,FROM,units="hour")>=hr))
                    if(!inherits(PossibleError, "error") && MAXRTS>FROMP && (is.null(time)||(difftime(MINRTS,FROMP,units="mins")>=time||exists("hr") && difftime(MINRTS,FROMP,units="hour")>=hr)))
                    {
                      ResultSet=data.frame()
                      for(MID in 1:length(MPIDS))
                      {
                        RSdf=data.frame();
                        Q=paste0("'",MPIDS[MID],"-",IsDerivedResult[1,"EQUIPMENTID"],"-%'",collapse=" OR ROWKEY like ");
                        MinMPID=paste(MPIDS[MID],"-",IsDerivedResult[1,"EQUIPMENTID"],sep="");
                        MaxMPID=paste(MPIDS[MID],"-",as.integer(IsDerivedResult[1,"EQUIPMENTID"])+1,sep="");
                        PossibleError <- tryCatch((RSdf=rimpala.query(paste("select ROWKEY,EQUIPMENTID,MODELPARAMETERID,RECORDEDTIMESTAMP,UPDATEDTIMESTAMP,VALUE from RHISTORYPARAMETERVALUE  where (rowkey>'",MinMPID,"' and rowkey<'",MaxMPID,"') and EQUIPMENTID='",IsDerivedResult[1,"EQUIPMENTID"],"' and MODELPARAMETERID='",MPIDS[MID],"' and updatedtimestamp >='",FROMP,"'",sep=""))),
                                                  error=function(e) e)
                        if(nrow(RSdf)>0)
                        {
                          colnames(RSdf)=c("ROWKEY","EQUIPMENTID","MODELPARAMETERID","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP","VALUE");
                          ResultSet=rbind(ResultSet,RSdf);  
                        }else{TFlag=1;rimpala.close();dbDisconnect(con);return();}
                        
                      } 
                    }else{TFlag=1;rimpala.close();dbDisconnect(con);return();}
                    
                    if(!inherits(PossibleError, "error") && length(ResultSet)!=0 && TFlag!=1)
                    {
                      colnames(ResultSet)=c("ROWKEY","EQUIPMENTID","MODELPARAMETERID","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP","VALUE")
                      #                 if(LFLAG==1 && length(FROMA)>0 && FROMA>FROMP)
                      #                 {
                      #                   Indx=ResultSet[,"UPDATEDTIMESTAMP"]>FROMA
                      #                   a=ResultSet[!Indx,c(3,6)]
                      #                   a=a[order(a[,1]),]
                      #                   LASTV=a[c(match(MPIDS,a[,"MODELPARAMETERID"])),"VALUE"]
                      #                   LASTV[is.na(LASTV)]=0
                      #                   ResultSet=ResultSet[Indx,]
                      #                 }
                      ResultSet[,"UPDATEDTIMESTAMP"]=gsub("\\.0","",ResultSet[,"UPDATEDTIMESTAMP"])
                      TST=gsub("\\.0","",MINRTS)
                    }else{
                      TFlag=1;rimpala.close();dbDisconnect(con);return();
                    }  
                  }
                  
                }else{TFlag=1;rimpala.close();dbDisconnect(con);return();}
              }
              info(logger,paste(IsDerivedResult[1,"MODELPARAMETERID"],"-",IsDerivedResult[1,"EQUIPMENTID"],"data fetched",sep=" "))
            }
            if(TFlag!=1)
            {
              if(DFlag!=1) 
              {  
                if(!is.null(TO) && !is.null(FROM))
                {
                  Sindex=!is.na(match(ResultSet[,"MODELPARAMETERID"],SMPID))
                  Eindex=!is.na(match(ResultSet[,"MODELPARAMETERID"],EMPID))
                  Sdf=ResultSet[Sindex,"RECORDEDTIMESTAMP"]
                  Edf=ResultSet[Eindex,"RECORDEDTIMESTAMP"]
                  nr=min(length(Edf),length(Sdf))
                  TST=max(Edf)
                  Nindex=ResultSet[,"RECORDEDTIMESTAMP"]>TST
                  RST=ResultSet[!(Sindex|Eindex|Nindex),]
                  MPIDS=MPIDS[is.na(match(MPIDS,c(SMPID,EMPID)))]
                  SMPIDS=MPIDS[is.na(match(MPIDS,OffMPIDS[,"ANALOGMODELPARAMETERID"]))]
                  BFlag=1;
                  
                }else{
                  if(CFlag==1)
                  {
                    Cindex=!is.na(match(ResultSet[,"MODELPARAMETERID"],CMPIDS))
                    Cdf=ResultSet[Cindex,]
                    TST=max(Cdf[,"RECORDEDTIMESTAMP"])
                    Nindex=ResultSet[,"RECORDEDTIMESTAMP"]>TST
                    RST=ResultSet[!(Cindex|Nindex),]
                    RST[,"UPDATEDTIMESTAMP"]=gsub("\\.0","",RST[,"UPDATEDTIMESTAMP"])
                    MPIDS=MPIDS[is.na(match(MPIDS,c(CMPIDS)))]
                    RECTS=c() 
                    Cdf[,"UPDATEDTIMESTAMP"]=gsub("\\.0","",Cdf[,"UPDATEDTIMESTAMP"])
                    for(C in 1:nrow(Cdf))
                    {
                      exp=Condition
                      exp=gsub(CMPIDS,Cdf[C,"VALUE"],exp)
                      if(eval(parse(text=paste0(exp))))
                      {
                        if(Cdf[C,"RECORDEDTIMESTAMP"]==Cdf[C,"UPDATEDTIMESTAMP"])
                        {
                          RECTS[length(RECTS)+1]=Cdf[C,"RECORDEDTIMESTAMP"];   
                        }else{
                          DateTime=seq((as.POSIXct(Cdf[C,"RECORDEDTIMESTAMP"])), as.POSIXct(Cdf[C,"UPDATEDTIMESTAMP"]), by="sec")
                          RECTS[(length(RECTS)+1):(length(RECTS)+length(DateTime))]=DateTime 
                        }
                        
                      }         
                    }
                  }
                  if(exists("hr")){
                    MRTS=min(ResultSet[,"RECORDEDTIMESTAMP"]);
                    if(nrow(Pointer)>0){MRTS=max(MRTS,Pointer[1,"POINTER"])}
                    mtg=as.integer(strftime(MRTS,"%M"))-mins/60;
                    if(mtg>=0)
                    {
                      MMRTS=(floor_date(as.POSIXlt(MRTS), 'hour')- (as.integer(strftime(MRTS,"%H"))%%hr)*3600)+mins                    
                    }else{
                      MMRTS=(floor_date(as.POSIXlt(MRTS), 'hour')-(as.integer(strftime(MRTS,"%H"))%%hr)*3600)-mins    
                    }
                    
                    MUTS=max(ResultSet[,"UPDATEDTIMESTAMP"])
                    mtg=as.integer(strftime(MUTS,"%M"))-mins/60
                    if(mtg>0)
                    {
                      MMUTS=(floor_date(as.POSIXlt(MUTS), 'hour')- (as.integer(strftime(MUTS,"%H"))%%hr)*3600)+mins+hr*3600                    
                    }else{
                      MMUTS=(floor_date(as.POSIXlt(MUTS), 'hour')-(as.integer(strftime(MUTS,"%H"))%%hr)*3600)-mins+hr*3600    
                    }
                  }
                  if(length(LMPIDS)>0)
                  {
                    if(exists("LMLMPID") && length(LMLMPID)>0 && pm=="ICT")
                    {
                      MIX=!is.na(match(ResultSet[,"MODELPARAMETERID"],LMLMPID))
                      MSubSet=ResultSet[MIX,];
                      MVAL=unique(MSubSet[,"VALUE"]);
                      MVALUES=paste(unlist(MVAL),collapse=",");
                      LWMPID=LMPIDS[is.na(match(LMPIDS,LMLMPID))]
                      IX=!is.na(match(ResultSet[,"MODELPARAMETERID"],LWMPID))
                      SubSet=ResultSet[IX,];
                      VAL=unique(SubSet[,"VALUE"]);
                      VALUES=paste(unlist(VAL),collapse=",");
                      ICTdf=suppressWarnings(dbGetQuery(con,paste0("select PRODUCTSERIALNUMBER,MOULDCODE,IDEALCYCLETIME from PRODUCTMACHINEMOULDMASTER where EQUIPMENTID=",IsDerivedResult[1,'EQUIPMENTID']," and MOULDCODE IN (",MVALUES,") and PRODUCTSERIALNUMBER IN (",VALUES,");")));
                      if(nrow(ICTdf)>0 && length(SubSet)>0)
                      {
                        PMDF=paste(ICTdf[,"PRODUCTSERIALNUMBER"],ICTdf[,"MOULDCODE"],sep="-");
                        LMM=1;
                        Rindex=c();
                        MVALUES=c();
                        PSVALUES=c();
                        for(LM in 1:nrow(SubSet))
                        {
                          if(SubSet[LM,"VALUE"]!=0 && LMM<=nrow(MSubSet))
                          {
                            if(SubSet[LM,"UPDATEDTIMESTAMP"]==MSubSet[LMM,"UPDATEDTIMESTAMP"]||SubSet[LM,"RECORDEDTIMESTAMP"]==MSubSet[LMM,"RECORDEDTIMESTAMP"])
                            {
                              valuesubset=ICTdf[which(paste(SubSet[LM,"VALUE"],MSubSet[LMM,"VALUE"],sep="-")==PMDF),"IDEALCYCLETIME"]
                              if(length(valuesubset)>0)
                              {
                                SubSet[LM,"VALUE"]=valuesubset;
                                #MVALUES=c(MVALUES,MSubSet[LM,"VALUE"]);
                                #PSVALUES=c(PSVALUES,SubSet[LM,"VALUE"]);
                              }else{
                                #MVALUES=c(MVALUES,MSubSet[LM,"VALUE"]);
                                #PSVALUES=c(PSVALUES,SubSet[LM,"VALUE"]);
                                index=rownames(SubSet[LM,]);
                                Rindex=c(Rindex,index);
                              }
                            }else{
                              Ind=which(SubSet[LM,"UPDATEDTIMESTAMP"]==MSubSet[,"UPDATEDTIMESTAMP"])
                              if(length(Ind)>0)
                              {
                                LMM=min(Ind[length(Ind)]);
                                valueict=ICTdf[which(paste(SubSet[LM,"VALUE"],MSubSet[LMM,"VALUE"],sep="-")==PMDF),"IDEALCYCLETIME"];
                                if(length(valueict)>0){SubSet[LM,"VALUE"]=valueict};
                              }else{
                                Ind=which(SubSet[LM,"RECORDEDTIMESTAMP"]==MSubSet[,"RECORDEDTIMESTAMP"])
                                if(length(Ind)>0)
                                {
                                  #LMM=Ind[length(Ind)]
                                  LMM=min(Ind[[MSubSet[Ind,"VALUE"]!=0]]);
                                  valueict=ICTdf[which(paste(SubSet[LM,"VALUE"],MSubSet[LMM,"VALUE"],sep="-")==PMDF),"IDEALCYCLETIME"];
                                  if(length(valueict)>0){SubSet[LM,"VALUE"]=valueict};
                                }else{
                                  Ind=c();CIndx=c();DIndx=c();BIndx=c();MinTs=SubSet[LM,"RECORDEDTIMESTAMP"];MaxTs=SubSet[LM,"UPDATEDTIMESTAMP"];
                                  CIndx=MSubSet[,"RECORDEDTIMESTAMP"]<MinTs & MSubSet[,"UPDATEDTIMESTAMP"]>=MaxTs
                                  DIndx=MSubSet[,"UPDATEDTIMESTAMP"]>=MinTs & MSubSet[,"UPDATEDTIMESTAMP"]<=MaxTs
                                  EIndx=MSubSet[,"RECORDEDTIMESTAMP"]>=MinTs & MSubSet[,"UPDATEDTIMESTAMP"]>=MaxTs & MSubSet[,"RECORDEDTIMESTAMP"]<=MaxTs
                                  BIndx=MSubSet[,"RECORDEDTIMESTAMP"]==MSubSet[,"UPDATEDTIMESTAMP"] & MSubSet[,"RECORDEDTIMESTAMP"]<=MaxTs & MSubSet[,"RECORDEDTIMESTAMP"]>=MinTs
                                  CIndx=CIndx|DIndx|EIndx|BIndx;
                                  if(length(CIndx)>0 && any(CIndx))
                                  { 
                                    LMM=which(CIndx)[MSubSet[which(CIndx),"VALUE"]!=0][1];
                                    valueict=ICTdf[which(paste(SubSet[LM,"VALUE"],MSubSet[LMM,"VALUE"],sep="-")==PMDF),"IDEALCYCLETIME"];
                                    if(length(valueict)>0){SubSet[LM,"VALUE"]=valueict};
                                  }else{
                                    VAL=ICTdf[which(paste(SubSet[LM,"VALUE"],MSubSet[nrow(MSubSet),"VALUE"],sep="-")==PMDF),"IDEALCYCLETIME"];
                                    if(length(VAL)>0){SubSet[LM,"VALUE"]=ICTdf[which(paste(SubSet[LM,"VALUE"],MSubSet[nrow(MSubSet),"VALUE"],sep="-")==PMDF),"IDEALCYCLETIME"];
                                    }else{index=rownames(SubSet[LM,]);
                                    Rindex=c(Rindex,index);
                                    }
                                  }
                                }
                              }
                            }
                            LMM=LMM+1;
                          }else{LMM=LMM+1;}
                          if(LMM>nrow(MSubSet))
                          {
                            LMM=LMM-1;
                          }
                        }
                        ResultSet[IX,]=SubSet;
                        NNZVAL=unique(SubSet[,"VALUE"]);
                        NZVAL=VAL[VAL!=0];
                        NZVAL=NZVAL[!is.na(match(NZVAL,NNZVAL))];
                        #if(nrow(ICTdf)!=length(NZVAL) && nrow(ICTdf)<length(NZVAL))
                        if(length(NZVAL)>0)
                        {
                          NZVAL=NZVAL[is.na(match(NZVAL,ICTdf[,"IDEALCYCLETIME"]))];
                          if(length(NZVAL)>0)
                          {
                            index=rownames(SubSet[!is.na(match(SubSet[,"VALUE"],NZVAL)),]);
                            Rindex=c(Rindex,index);
                          }
                        }
                        #                     NZVAL=MVAL[MVAL!=0];
                        #                     NZVAL=NZVAL[is.na(match(NZVAL,MVALUES))]
                        #                     if(nrow(ICTdf)!=length(NZVAL) && nrow(ICTdf)<length(NZVAL))
                        #                     {
                        #                       NZVAL=NZVAL[NZVAL!=ICTdf[,"MOULDCODE"]];
                        #                       if(length(NZVAL)>0)
                        #                       {
                        #                         index=rownames(MSubSet[!is.na(match(MSubSet[,"VALUE"],NZVAL)),]);
                        #                         Rindex=c(Rindex,index);
                        #                       }
                        #                     }
                        if(length(Rindex)>0){ResultSet=ResultSet[-as.integer(Rindex),];}
                        
                        
                      }else{
                        index=rownames(SubSet[SubSet[,"VALUE"]!=0,]);
                        index=c(index,rownames(MSubSet[MSubSet[,"VALUE"]!=0,]));
                        ResultSet=ResultSet[-as.integer(index),]
                      }
                      
                      
                    }else{
                      IX=!is.na(match(ResultSet[,"MODELPARAMETERID"],LMPIDS))
                      SubSet=ResultSet[IX,];
                      VAL=unique(SubSet[,"VALUE"]);
                      VALUES=paste(unlist(VAL),collapse=",");
                      if(pm=="STW")
                      {
                        LMdf=suppressWarnings(dbGetQuery(con,paste0("select SERIALNUMBER,STANDARDWEIGHT from PRODUCTMASTER where SERIALNUMBER IN (",VALUES,");")));
                      }
                      if(nrow(LMdf)>0 && length(SubSet)>0)
                      {
                        colnames(LMdf)=c("CODE","VALUE")
                        for(dfv in 1:nrow(LMdf))
                        {
                          SubSet[which(SubSet[,"VALUE"]==LMdf[dfv,"CODE"]),"VALUE"]=LMdf[dfv,"VALUE"];
                        }
                        ResultSet[IX,]=SubSet;
                        NZVAL=VAL[VAL!=0];
                        NZVAL=NZVAL[is.na(match(NZVAL,LMdf[,"CODE"]))];
                        if(length(NZVAL)>0)
                        {
                          index=rownames(SubSet[!is.na(match(SubSet[,"VALUE"],NZVAL)),]);
                          ResultSet=ResultSet[-as.integer(index),]
                        }
                        
                      }else{
                        index=rownames(SubSet[SubSet[,"VALUE"]!=0,]);
                        if(length(index)>0){ResultSet=ResultSet[-as.integer(index),]};
                        
                      }
                      
                    }
                    
                  }
                  
                }
                for(M in 1:length(MPIDS))
                {
                  if((exists("BFlag") && BFlag==1)||(exists("CFlag") && CFlag==1))
                  { RS=RST[which(RST[,"MODELPARAMETERID"]==MPIDS[M]),]
                  }else{
                    RS=ResultSet[which(ResultSet[,"MODELPARAMETERID"]==MPIDS[M]),]
                  }
                  if(nrow(RS)>0 && TFlag!=1)
                  {
                    if(exists("hr"))
                    {
                      FIndx=sample(c(F),nrow(RS), replace = TRUE);
                      CIndx=c();
                      DIndx=c();
                      EIndx=c();
                      BIndx=c();
                      RS1=data.frame();
                      TS=c()
                      RTS=c();
                      MRTS=MMRTS;
                      MUTS=MMUTS;
                      while(MRTS!=MUTS)
                      {
                        MaxTs=as.character(as.POSIXlt(MRTS)+hr*3600);
                        CIndx=RS[,"RECORDEDTIMESTAMP"]<MRTS & RS[,"UPDATEDTIMESTAMP"]>=MaxTs
                        DIndx=RS[,"UPDATEDTIMESTAMP"]>MRTS & RS[,"UPDATEDTIMESTAMP"]<=MaxTs
                        EIndx=RS[,"RECORDEDTIMESTAMP"]>MRTS & RS[,"UPDATEDTIMESTAMP"]>=MaxTs & RS[,"RECORDEDTIMESTAMP"]<=MaxTs
                        BIndx=RS[,"RECORDEDTIMESTAMP"]==RS[,"UPDATEDTIMESTAMP"] & RS[,"RECORDEDTIMESTAMP"]<=MaxTs & RS[,"RECORDEDTIMESTAMP"]>MRTS
                        CIndx=CIndx|DIndx|EIndx|BIndx;
                        if(any(CIndx))
                        { 
                          RS1=rbind(RS1,RS[CIndx,]);
                          TS=c(TS,rep(as.character(as.POSIXlt(MRTS)+15*60),sum(CIndx)));
                        }
                        MRTS=MaxTs;
                      }
                      if(length(RS1)>0)
                      {
                        RS=RS1
                      }
                      RTS=unique(TS);
                      
                      
                      
                      
                      
                    }else{
                      if(exists("BFlag") && BFlag==1)
                      { 
                        TS=c()
                        for(B in 1:length(nr))
                        {
                          SSindex=RS[,"RECORDEDTIMESTAMP"]>=Sdf[B] & RS[,"RECORDEDTIMESTAMP"]<=Edf[B]
                          RS=RS[SSindex,]
                          TS[SSindex]=rep(as.character(as.POSIXct(Edf[B])-60),sum(SSindex))
                        }
                        TS=TS[!is.na(TS)]
                        RTS=unique(TS)
                      }else{
                        if(exists("CFlag") && CFlag==1)
                        {
                          TS=c()
                          TSindex=F;
                          for(B in 1:length(RECTS))
                          {
                            SSindex=RS[,"RECORDEDTIMESTAMP"]==RECTS[B] | RS[,"UPDATEDTIMESTAMP"]==RECTS[B]
                            if(any(SSindex)>0)
                            {
                              TS[SSindex]=rep(RECTS[B],sum(SSindex))
                              TSindex=TSindex|SSindex
                            }
                            
                          }
                          RS=RS[TSindex,]
                          RS=rbind(RS,RS[2:nrow(RS),])
                          TS=TS[!is.na(TS)]
                          TS=c(TS[2],TS[2:length(TS)],TS[2:length(TS)])
                          RTS=unique(TS)
                          TST=RTS[length(RTS)-1]
                          
                          
                        }else{
                          TS<- as.POSIXct(RS$UPDATEDTIMESTAMP, format="%Y-%m-%d %H:%M")
                          TS=as.character(align.time(TS,time* 60))
                          RTS=unique(TS)
                        }
                      }
                    }
                    
                    if((!is.null(Condition) || length(Condition)>3) && CFlag!=1)
                    {
                      RIndex=eval(parse(text=gsub(MPIDS[M],"RS[,'VALUE']",Condition)))
                      RS=RS[RIndex,]
                      TS=TS[RIndex]
                      RTS=unique(TS)              
                      
                    }
                    if((Fname %in% c("WeightedAverage","TimeTotalized","Cumulative")) && nrow(RS)>0) 
                    {
                      #             if(length(RTS)>0 && CFlag!=1)
                      #             {   
                      #               MaxTS=as.character(RTS[is.na(match(RTS,RS$RECORDEDTIMESTAMP))])
                      #               if(length(MaxTS)>0)
                      #               {
                      #                 RST=data.frame()
                      #                 for(i in 2:(length(MaxTS)))
                      #                 {
                      #                   TS[length(TS)+1]=as.character(MaxTS[i])
                      #                   if(i!=length(MaxTS))
                      #                   {#df=RS[max(which(MaxTS[i+1]>as.character(RS$RECORDEDTIMESTAMP))),]
                      #                     df=RS[max(which(MaxTS[i]>=as.character(RS$UPDATEDTIMESTAMP))),]
                      #                   }else{
                      #                     df=RS[nrow(RS),]
                      #                   }
                      #                   cf=cbind(df[1,1:3],as.character(MaxTS[i]),as.character(MaxTS[i]),df[1,6])
                      #                   colnames(cf)=colnames(RS)
                      #                   RST=rbind(RST,cf)
                      #                 }
                      #                 RS=rbind(RS,RST)
                      #               }
                      #               #RS=RS[order(RS[,4]),]  
                      #             }
                      if(Fname!="Cumulative")
                      {
                        df=data.frame();
                        Res=c();
                        for(i in 1:length(RTS))
                        {
                          df=RS[which(TS==RTS[i]),]
                          if(nrow(df)>1)
                          {
                            Res[i]=eval(parse(text=paste0(Fname,"(df)")))   
                          }
                          
                        }
                        aggdf=cbind(RTS,Res);
                        
                      }else{
                        if(Fname=='TTime')
                        {
                          diff=as.POSIXct(RS$UPDATEDTIMESTAMP,format='%Y-%m-%d %H:%M:%S')-as.POSIXct(RS$RECORDEDTIMESTAMP,format='%Y-%m-%d %H:%M:%S')+1
                          Fname='sum';
                          RS$VALUE=diff;
                        }
                        aggdf=aggregate(RS$VALUE,by=list(TS),Fname)}
                      
                    }else{if(length(TS)>0 && nrow(RS)>0){aggdf=aggregate(RS$VALUE,by=list(TS),Fname)} }
                    
                    if(length(TS)>0 && nrow(RS)>0)
                    {
                      colnames(aggdf)[1:2]=c("DTime",MPIDS[M])
                      if(M>1)
                      {
                        df=merge(df, aggdf, by=c("DTime"),all = TRUE)
                      }else{
                        df=aggdf
                      }
                    }
                    
                  }else{TFlag=1;rimpala.close();dbDisconnect(con);return();}
                }
                rm(ResultSet)
                if((ncol(df)-1)==length(MPIDS) && TFlag!=1)
                {
                  Res=c()
                  DPH=data.frame()
                  Index=complete.cases(df)
                  if(length(Index)>1 && any(Index))
                  {
                    df=df[Index,];  
                  }
                  if(nrow(df)>0){Res=ExEVALHM(df,ExpressionMM,MPIDS);}
                  #Res=Res[Index]
                  if(nrow(df)>0 && length(Res)>0)
                  {
                    if(exists("CFlag") && CFlag!=1 && is.null(TST))
                    {TST=df[nrow(df),"DTime"];}
                    RKH=suppressWarnings(paste(IsDerivedResult[1,"MODELPARAMETERID"],IsDerivedResult[1,"EQUIPMENTID"],df[,"DTime"],sep="-"));
                    #DPH=suppressWarnings(data.frame(cbind(RKH,IsDerivedResult[1,"MODELPARAMETERID"],IsDerivedResult[1,"EQUIPMENTID"],Res,as.character(RTS),as.character(RTS))))
                    DPH=suppressWarnings(data.frame(cbind(RKH,IsDerivedResult[1,"MODELPARAMETERID"],IsDerivedResult[1,"EQUIPMENTID"],Res,df[,"DTime"],df[,"DTime"]),stringsAsFactors=FALSE));
                    if(exists("time")){if(exists("hr")){TST=as.character(as.POSIXlt(df[nrow(df),"DTime"])-hr*4500);
                    }else{if(nrow(df)>1){TST=df[nrow(df)-1,"DTime"];}else{TFlag=1;rimpala.close();dbDisconnect(con);return();}     }}
                  }else{TFlag=1;rimpala.close();dbDisconnect(con);return();}
                }else{TFlag=1;rimpala.close();dbDisconnect(con);return();}
              }else{
                RS=ResultSet
                #RS=ResultSet[which(ResultSet[,"MODELPARAMETERID"]==MPIDS[M]),]
                if(length(MPIDS)==length(unique(RS[,"MODELPARAMETERID"])))
                {
                  RS[,"UPDATEDTIMESTAMP"]=gsub("\\.0","",RS[,"UPDATEDTIMESTAMP"])
                  DateTime=c()
                  TS=c()
                  if(exists("hr"))
                  {
                    RIndex=which(!(RS[,"UPDATEDTIMESTAMP"]==RS[,"RECORDEDTIMESTAMP"]))
                    if(length(RIndex)>0)
                    {
                      for(ID in 1:length(RIndex))
                      {
                        DateTime=seq((as.POSIXct(RS[RIndex[ID],"RECORDEDTIMESTAMP"])+1), as.POSIXct(RS[RIndex[ID],"UPDATEDTIMESTAMP"]), by="sec")
                        PDF=suppressWarnings(cbind(paste(RS[RIndex[ID],"MODELPARAMETERID"],RS[RIndex[ID],"EQUIPMENTID"],DateTime,sep="-"),RS[RIndex[ID],2:3],as.character(DateTime),as.character(DateTime),RS[RIndex[ID],"VALUE"]))
                        colnames(PDF)=c("ROWKEY","EQUIPMENTID","MODELPARAMETERID","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP","VALUE")
                        RS=rbind(RS,PDF)
                      }
                    }
                    TS<- as.POSIXct(RS$RECORDEDTIMESTAMP, format="%Y-%m-%d %H")
                    TS=cut(TS, breaks=paste(hr,"hour"))
                    TS=as.POSIXlt(TS)+(hr-1)*60*60
                    RTS=unique(TS)
                  }else
                  {
                    if(!is.null(time))
                    {
                      RIndex=which(!(RS[,"UPDATEDTIMESTAMP"]==RS[,"RECORDEDTIMESTAMP"]))
                      if(length(RIndex)>0)
                      {
                        for(ID in 1:length(RIndex))
                        {
                          DateTime=seq((as.POSIXct(RS[RIndex[ID],"RECORDEDTIMESTAMP"])+1), as.POSIXct(RS[RIndex[ID],"UPDATEDTIMESTAMP"]), by="sec")
                          PDF=suppressWarnings(cbind(paste(RS[RIndex[ID],"MODELPARAMETERID"],RS[RIndex[ID],"EQUIPMENTID"],DateTime,sep="-"),RS[RIndex[ID],2:3],as.character(DateTime),as.character(DateTime),RS[RIndex[ID],"VALUE"]))
                          colnames(PDF)=c("ROWKEY","EQUIPMENTID","MODELPARAMETERID","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP","VALUE")
                          RS=rbind(RS,PDF)
                        }
                      }
                      TS<- as.POSIXct(RS$RECORDEDTIMESTAMP, format="%Y-%m-%d %H:%M")
                      TS=align.time(TS, time* 60) 
                      RTS=unique(TS)
                    }else{ 
                      
                      Sindex=!is.na(match(ResultSet[,"MODELPARAMETERID"],SMPID))
                      Eindex=!is.na(match(ResultSet[,"MODELPARAMETERID"],EMPID))
                      Sdf=ResultSet[Sindex,"RECORDEDTIMESTAMP"]
                      Edf=ResultSet[Eindex,"RECORDEDTIMESTAMP"]
                      nr=min(length(Edf),length(Sdf))
                      TST=max(Edf)
                      Nindex=ResultSet[,"RECORDEDTIMESTAMP"]>TST
                      RS=ResultSet[!(Sindex|Eindex|Nindex),]
                      RS[,"UPDATEDTIMESTAMP"]=gsub("\\.0","",RS[,"UPDATEDTIMESTAMP"])
                      MPIDS=MPIDS[is.na(match(MPIDS,c(SMPID,EMPID)))]
                      SMPIDS=MPIDS[is.na(match(MPIDS,OffMPIDS[,"ANALOGMODELPARAMETERID"]))]
                      RIndex=which(!(RS[,"UPDATEDTIMESTAMP"]==RS[,"RECORDEDTIMESTAMP"]))
                      if(length(RIndex)>0)
                      {
                        for(ID in 1:length(RIndex))
                        {
                          DateTime=seq((as.POSIXct(RS[RIndex[ID],"RECORDEDTIMESTAMP"])+1), as.POSIXct(RS[RIndex[ID],"UPDATEDTIMESTAMP"]), by="sec")
                          PDF=suppressWarnings(cbind(paste(RS[RIndex[ID],"MODELPARAMETERID"],RS[RIndex[ID],"EQUIPMENTID"],DateTime,sep="-"),RS[RIndex[ID],2:3],as.character(DateTime),as.character(DateTime),RS[RIndex[ID],"VALUE"]))
                          colnames(PDF)=c("ROWKEY","EQUIPMENTID","MODELPARAMETERID","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP","VALUE")
                          RS=rbind(RS,PDF)
                        }
                      }
                      for(B in 1:length(nr))
                      {
                        SSindex=RS[,"RECORDEDTIMESTAMP"]>=Sdf[B] & RS[,"RECORDEDTIMESTAMP"]<=Edf[B]
                        TS[SSindex]=rep(as.character(as.POSIXct(Edf[B])-60),sum(SSindex))
                      }
                      RTS=unique(TS)
                      
                    }
                  }
                  Expression=paste(Fname,"(ExpressionM,Condition,RS,RTS,TS,MPIDS,IsDerivedResult)",sep="")
                  DPH=eval(parse(text=paste0(Expression)))
                  
                }else{
                  TFlag=1;rimpala.close();dbDisconnect(con);return();
                }
              }
              rimpala.close();
              if(TFlag!=1)  
              {
                if(nrow(DPH)>0)
                {
                  Flag=WriteF(DPH,paste(IsDerivedResult[1,"MODELPARAMETERID"],IsDerivedResult[1,"EQUIPMENTID"],sep="-"))
                  if(Flag==1){suppressWarnings(dbGetQuery(con,paste0("insert into POINTER (MODELPARAMETERID,EQUIPMENTID,POINTER) values (",IsDerivedResult[1,"MODELPARAMETERID"],",",IsDerivedResult[1,"EQUIPMENTID"],",'",TST,"') ON DUPLICATE KEY UPDATE POINTER='",TST,"';")))}
                }
                
                
              }
              
            }
          },error = function(err) { error(logger,paste("MPID=",IsDerivedResult[1,"MODELPARAMETERID"],"and EQUIPMENTID=",IsDerivedResult[1,"EQUIPMENTID"],err,sep=" "));err; })
          try(dbDisconnect(con),silent=T);
        }
        
        Monthly<-function(ExpressionM,Fname,Shift=NULL,Condition=NULL,IsDerivedResult,Pointer,logger,LFLAG)
        {
          TFlag=0;
          MPIDS=c();
          if(length(ExpressionM)>0)
          {
            rimpala.init(libs="/opt/cloudera/parcels/IMPALA/lib/impala/lib/")
            rimpala.connect("x.x.x.x","21050")
            con <- dbConnect(dbDriver("MySQL"),user=userDB,password=passDB,dbname=DBName,host=DBHost,port=3306)
            cm <- gregexpr("CM[0-9]*", ExpressionM, perl=TRUE)
            if(any(cm[[1]]!=-1))
            {
              CMMPIDS=sort(unique(unlist(regmatches(ExpressionM,cm))));
              CMMPIDS=gsub("CM","",CMMPIDS);
              CMMPVAL=suppressWarnings(dbGetQuery(con,paste0("SELECT MODELPARAMETERID,PARAMETERVALUE FROM COMMISSIONINGPOINT where EQUIPMENTID=",IsDerivedResult[1,"EQUIPMENTID"]," and MODELPARAMETERID IN (",paste(CMMPIDS,collapse=","),");")))
              if(nrow(CMMPVAL)>0)
              {
                for(c in 1:nrow(CMMPVAL))
                {
                  ExpressionM=gsub(paste("CM",CMMPVAL[c,1],sep=""),CMMPVAL[c,2],ExpressionM)
                }
                ExpressionM=gsub("CM","",ExpressionM);
                
              }else{
                ExpressionM=gsub("CM","HM",ExpressionM);
              }
            }
            hm <- gregexpr("HM[0-9]*", ExpressionM, perl=TRUE)
            if(any(hm[[1]]!=-1))
            {
              HMPIDS=sort(unique(unlist(regmatches(ExpressionM,hm))))
              HMPIDS=gsub("HM","",HMPIDS)
              MPIDS=c(MPIDS,HMPIDS)
            }
            LASTV=c()
            Fts=0;
            OffMPIDS=suppressWarnings(dbGetQuery(con,paste0("SELECT ANALOGMODELPARAMETERID FROM EQUIPMENTPARAMETER WHERE ISONSCREEN=1 AND EQUIPMENTID=",IsDerivedResult[1,"EQUIPMENTID"]," AND  ANALOGMODELPARAMETERID IN (",paste(MPIDS,collapse=","),");")))  
            if(LFLAG==0 && nrow(OffMPIDS)>0){
              Q=paste0("'",unlist(OffMPIDS),"-",IsDerivedResult[1,"EQUIPMENTID"],"'",collapse=" OR ROWKEY=")
              PossibleError <- tryCatch((df=dbGetQuery(con,paste("select * from LATESTPARAMETERVALUE where ROWKEY =",Q,sep=""))),
                                        error=function(e) e
              )
              colnames(df)=c("ROWKEY","MODELPARAMETERID","EQUIPMENTID","VALUE","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP")
              for(Oind in 1:nrow(OffMPIDS))
              {
                val=df[which(OffMPIDS[Oind]==df[,"MODELPARAMETERID"]),"VALUE"]
                ExpressionM=gsub(paste("HM",OffMPIDS[Oind],sep=""),val,ExpressionM)
                MPIDS=MPIDS[is.na(match(MPIDS,OffMPIDS[Oind]))]
              }
            }
            PossibleError <- tryCatch({
              if(nrow(Pointer)==0)
              { 
                Q=paste0("'",unlist(MPIDS),"-",IsDerivedResult[1,"EQUIPMENTID"],"'",collapse=" OR ROWKEY=")
                PossibleError <- tryCatch((df=dbGetQuery(con,paste("select ROWKEY,EQUIPMENTID,MODELPARAMETERID,RECORDEDTIMESTAMP,UPDATEDTIMESTAMP,VALUE from LATESTPARAMETERVALUE where ROWKEY =",Q,sep=""))),
                                          error=function(e) e
                ) 
                if(!inherits(PossibleError, "error") && nrow(df)==length(MPIDS))
                {
                  Cdate=Sys.Date();
                  ResultSet=data.frame()
                  for(MID in 1:length(MPIDS))
                  {
                    RSdf=data.frame();
                    Q=paste0("'",MPIDS[MID],"-",IsDerivedResult[1,"EQUIPMENTID"],"-%'",collapse=" OR ROWKEY like ");
                    MinMPID=paste(MPIDS[MID],"-",IsDerivedResult["EQUIPMENTID"],sep="");
                    MaxMPID=paste(MPIDS[MID],"-",as.integer(IsDerivedResult["EQUIPMENTID"])+1,sep="");
                    PossibleError <- tryCatch((RSdf=rimpala.query(paste("select ROWKEY,EQUIPMENTID,MODELPARAMETERID,RECORDEDTIMESTAMP,UPDATEDTIMESTAMP,VALUE from RHISTORYPARAMETERVALUE  where (rowkey>'",MinMPID,"' and rowkey<'",MaxMPID,"') and EQUIPMENTID='",IsDerivedResult[1,"EQUIPMENTID"],"' and MODELPARAMETERID='",MPIDS[MID],"'",sep=""))),
                                              error=function(e) e)
                    if(nrow(RSdf)>0)
                    {
                      colnames(RSdf)=c("ROWKEY","EQUIPMENTID","MODELPARAMETERID","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP","VALUE");
                      ResultSet=rbind(ResultSet,RSdf);  
                    }else{TFlag=1;rimpala.close();dbDisconnect(con);return();}
                    
                  }
                  if(!inherits(PossibleError, "error"))
                  {
                    colnames(ResultSet)=c("ROWKEY","EQUIPMENTID","MODELPARAMETERID","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP","VALUE")
                    LASTV[1:length(MPIDS)]=0
                    ResultSet[,"RECORDEDTIMESTAMP"]=gsub("\\.0","",ResultSet[,"RECORDEDTIMESTAMP"])
                    TST=ResultSet[nrow(ResultSet),"UPDATEDTIMESTAMP"]
                  }else{
                    TFlag=1;rimpala.close();dbDisconnect(con);return();
                  }
                }else{
                  TFlag=1;rimpala.close();dbDisconnect(con);return();
                }
                if(length(ResultSet)==0){TFlag=1;rimpala.close();dbDisconnect(con);return();}
                
                
                
              }else{
                
                Cdate=Sys.Date();
                FROM=Pointer[,"POINTER"]
                PossibleError <- tryCatch((FROMA=dbGetQuery(con,paste("select RECORDEDTIMESTAMP from LATESTPARAMETERVALUE where ROWKEY ='",IsDerivedResult[1,"MODELPARAMETERID"],"-",IsDerivedResult[1,"EQUIPMENTID"],"'",sep=""))[[1]]),
                                          error=function(e) e)
                if(!inherits(PossibleError, "error"))
                {
                  Q=paste0("'",unlist(MPIDS),"-",IsDerivedResult[1,"EQUIPMENTID"],"'",collapse=" OR ROWKEY=")
                  PossibleError <- tryCatch((df=dbGetQuery(con,paste("select ROWKEY,EQUIPMENTID,MODELPARAMETERID,RECORDEDTIMESTAMP,UPDATEDTIMESTAMP,VALUE from LATESTPARAMETERVALUE where ROWKEY =",Q," and recordedtimestamp >='",FROM,"'",sep=""))),
                                            error=function(e) e
                  )
                  if(!inherits(PossibleError, "error") && nrow(df)==length(MPIDS))
                  {
                    #MTS=max(df[,"UPDATEDTIMESTAMP"])
                    MINRTS=min(df[,"RECORDEDTIMESTAMP"])
                    TST=min(MINRTS,as.character(Cdate))
                  }else{
                    TFlag=1;rimpala.close();dbDisconnect(con);return();
                  }
                  #if(TST>FROMA && FROM<=FROMA)
                  #if(TFlag!=1 && TST>FROM)
                  if(TFlag!=1)
                  {
                    ResultSet=data.frame()
                    for(MID in 1:length(MPIDS))
                    {
                      RSdf=data.frame();
                      Q=paste0("'",MPIDS[MID],"-",IsDerivedResult[1,"EQUIPMENTID"],"-%'",collapse=" OR ROWKEY like ");
                      MinMPID=paste(MPIDS[MID],"-",IsDerivedResult[1,"EQUIPMENTID"],sep="");
                      MaxMPID=paste(MPIDS[MID],"-",as.integer(IsDerivedResult[1,"EQUIPMENTID"])+1,sep="");
                      PossibleError <- tryCatch((RSdf=rimpala.query(paste("select ROWKEY,EQUIPMENTID,MODELPARAMETERID,RECORDEDTIMESTAMP,UPDATEDTIMESTAMP,VALUE from RHISTORYPARAMETERVALUE  where (rowkey>'",MinMPID,"' and rowkey<'",MaxMPID,"') and EQUIPMENTID='",IsDerivedResult[1,"EQUIPMENTID"],"' and MODELPARAMETERID='",MPIDS[MID],"' and updatedtimestamp >='",FROM,"'",sep=""))),
                                                error=function(e) e)
                      if(nrow(RSdf)>0)
                      {
                        colnames(RSdf)=c("ROWKEY","EQUIPMENTID","MODELPARAMETERID","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP","VALUE");
                        ResultSet=rbind(ResultSet,RSdf);  
                      }else{TFlag=1;rimpala.close();dbDisconnect(con);return();}
                      
                    } 
                    if(!inherits(PossibleError, "error"))
                    {
                      colnames(ResultSet)=c("ROWKEY","EQUIPMENTID","MODELPARAMETERID","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP","VALUE")  
                      #                 if(length(FROMA)>0 && FROMA>FROM)
                      #                 {
                      #                 Indx=ResultSet[,"RECORDEDTIMESTAMP"]>FROMA;
                      #                 a=ResultSet[!Indx,c(3,6)]
                      #                 a=a[order(a[,1]),]
                      #                 LASTV=a[c(match(MPIDS,a[,"MODELPARAMETERID"])),"VALUE"]
                      #                 LASTV[is.na(LASTV)]=0;
                      #                 ResultSet=ResultSet[Indx,];
                      #                 }
                      ResultSet[,"RECORDEDTIMESTAMP"]=gsub("\\.0","",ResultSet[,"RECORDEDTIMESTAMP"]);
                      Fts=1;
                    }else{
                      TFlag=1;
                    }               
                    
                  }else{
                    TFlag=1;rimpala.close();dbDisconnect(con);return();
                  }
                  if(TFlag!=1 && length(ResultSet)==0)
                  {
                    TFlag=1;rimpala.close();dbDisconnect(con);return();
                  }
                  
                }else{
                  TFlag=1;rimpala.close();dbDisconnect(con);return();
                }
                
              }  
              rimpala.close();
              if(TFlag!=1)
              {
                if(!is.null(Shift))
                { SETIME=ShiftSETIME(Shift,IsDerivedResult[1,"EQUIPMENTID"]);
                if(length(SETIME)>0){
                  Shifttime=strftime(SETIME[1],"%H:%M:%S")
                  if(strftime(Sys.time(),"%H:%M:%S")>Shifttime)
                  {STDATE=paste(Sys.Date(),Shifttime,sep=" ");
                  }else{STDATE=paste(Sys.Date()-1,Shifttime,sep=" ");
                  }
                }
                }else{
                  STDATE=paste(Sys.Date(),"00:00:00",sep=" ");
                }
                for(M in 1:length(MPIDS))
                {
                  RS=ResultSet[which(ResultSet[,"MODELPARAMETERID"]==MPIDS[M]),]
                  if(nrow(RS)>0 && TFlag!=1)
                  {
                    RS=RS[RS[,"UPDATEDTIMESTAMP"]<STDATE,];
                    MTS=RS[nrow(RS),"UPDATEDTIMESTAMP"];
                    Months=strftime(RS$UPDATEDTIMESTAMP,"%Y-%m")
                    aggdf=aggregate(RS$VALUE, by=list(Months),Fname)
                    colnames(aggdf)[1:2]=c("Month",MPIDS[M])
                    if(M>1){
                      df=merge(df, aggdf, by=c("Month"),all.x = TRUE)
                    }else{df=aggdf;}
                  }else{ TFlag=1;rimpala.close();dbDisconnect(con);return();}
                }  
                if(((ncol(df)-1)==length(MPIDS)) && TFlag!=1)
                {
                  DPH=data.frame()
                  Res=c()
                  Index=complete.cases(df)
                  if(length(Index)>1 && any(Index))
                  {df=df[Index,]}
                  if(nrow(df)>0){Res=ExEVAL(df,ExpressionM,MPIDS);}
                  #Res=Res[Index]
                  if(nrow(df)>0 && length(Res)>0)
                  {
                    RTS=paste(df[,"Month"],"-01 00:00:00",sep="");
                    
                    TST=RTS[length(RTS)];
                    MDate=strftime(MTS,"%Y-%m-%d");
                    MTS=paste(MDate,"23:59:59")
                    if(length(RTS)>1)
                    {
                      if(nrow(df)>1)
                      { 
                        UTS=c();
                        for(U in 1:nrow(df))
                        {
                          UTS[U]=max(ResultSet[grep(df[U,'Month'],ResultSet[,"UPDATEDTIMESTAMP"]),"UPDATEDTIMESTAMP"])
                        }
                        
                      }else{UTS=c(RTS[1:(length(RTS)-1)],MTS);}
                      
                    }else{UTS=MTS;}
                    RKH=suppressWarnings(paste(IsDerivedResult[1,"MODELPARAMETERID"],IsDerivedResult[1,"EQUIPMENTID"],RTS,sep="-"));
                    DPH=data.frame(cbind(RKH,IsDerivedResult[1,"MODELPARAMETERID"],IsDerivedResult[1,"EQUIPMENTID"],Res,RTS,UTS),stringsAsFactors=FALSE)
                  }else{TFlag=1;rimpala.close();dbDisconnect(con);return();}
                }else{TFlag=1;rimpala.close();dbDisconnect(con);return();}
              }
            },warning = function(war){ warn(logger,paste(ExpressionM,"for",IsDerivedResult[1,"MODELPARAMETERID"],"&",IsDerivedResult[1,"EQUIPMENTID"],war,sep=" "));war;}, 
            error = function(err) { error(logger,paste(ExpressionM,"for",IsDerivedResult[1,"MODELPARAMETERID"],"&",IsDerivedResult[1,"EQUIPMENTID"],err,sep=" "));err; })
            
            rimpala.close();
            if(TFlag!=1 && !inherits(PossibleError, "error") && !inherits(PossibleError, "warn") && nrow(DPH)>0)  
            {
              Flag=WriteF(DPH,paste(IsDerivedResult[1,"MODELPARAMETERID"],IsDerivedResult[1,"EQUIPMENTID"],sep="-"))
              if(Flag==1){suppressWarnings(dbGetQuery(con,paste0("insert into POINTER (MODELPARAMETERID,EQUIPMENTID,POINTER) values (",IsDerivedResult[1,"MODELPARAMETERID"],",",IsDerivedResult[1,"EQUIPMENTID"],",'",TST,"') ON DUPLICATE KEY UPDATE POINTER='",TST,"';")))}
            }
            dbDisconnect(con)        
          }
        }
        
        MTY<-function(ExpressionM,Fname,Shift=NULL,Condition=NULL,IsDerivedResult,Pointer,logger,LFLAG)
        {
          TFlag=0;
          MPIDS=c();
          if(length(ExpressionM)>0)
          {
            rimpala.init(libs="/opt/cloudera/parcels/IMPALA/lib/impala/lib/")
            rimpala.connect("x.x.x.x","21050")
            con <- dbConnect(dbDriver("MySQL"),user=userDB,password=passDB,dbname=DBName,host=DBHost,port=3306)
            cm <- gregexpr("CM[0-9]*", ExpressionM, perl=TRUE)
            if(any(cm[[1]]!=-1))
            {
              CMMPIDS=sort(unique(unlist(regmatches(ExpressionM,cm))));
              CMMPIDS=gsub("CM","",CMMPIDS);
              CMMPVAL=suppressWarnings(dbGetQuery(con,paste0("SELECT MODELPARAMETERID,PARAMETERVALUE FROM COMMISSIONINGPOINT where EQUIPMENTID=",IsDerivedResult[1,"EQUIPMENTID"]," and MODELPARAMETERID IN (",paste(CMMPIDS,collapse=","),");")))
              if(nrow(CMMPVAL)>0)
              {
                for(c in 1:nrow(CMMPVAL))
                {
                  ExpressionM=gsub(paste("CM",CMMPVAL[c,1],sep=""),CMMPVAL[c,2],ExpressionM)
                }
                ExpressionM=gsub("CM","",ExpressionM);
                
              }else{
                ExpressionM=gsub("CM","HM",ExpressionM);
              }
            }
            hm <- gregexpr("HM[0-9]*", ExpressionM, perl=TRUE)
            if(any(hm[[1]]!=-1))
            {
              HMPIDS=sort(unique(unlist(regmatches(ExpressionM,hm))))
              HMPIDS=gsub("HM","",HMPIDS)
              MPIDS=c(MPIDS,HMPIDS)
            }
            LASTV=c()
            Fts=0;
            OffMPIDS=suppressWarnings(dbGetQuery(con,paste0("SELECT ANALOGMODELPARAMETERID FROM EQUIPMENTPARAMETER WHERE ISONSCREEN=1 AND EQUIPMENTID=",IsDerivedResult[1,"EQUIPMENTID"]," AND  ANALOGMODELPARAMETERID IN (",paste(MPIDS,collapse=","),");")))  
            if(LFLAG==0 && nrow(OffMPIDS)>0){
              Q=paste0("'",unlist(OffMPIDS),"-",IsDerivedResult[1,"EQUIPMENTID"],"'",collapse=" OR ROWKEY=")
              PossibleError <- tryCatch((df=dbGetQuery(con,paste("select * from LATESTPARAMETERVALUE where ROWKEY =",Q,sep=""))),
                                        error=function(e) e
              )
              colnames(df)=c("ROWKEY","MODELPARAMETERID","EQUIPMENTID","VALUE","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP")
              for(Oind in 1:nrow(OffMPIDS))
              {
                val=df[which(OffMPIDS[Oind]==df[,"MODELPARAMETERID"]),"VALUE"]
                ExpressionM=gsub(paste("HM",OffMPIDS[Oind],sep=""),val,ExpressionM)
                MPIDS=MPIDS[is.na(match(MPIDS,OffMPIDS[Oind]))]
              }
            }
            PossibleError <- tryCatch({
              if(nrow(Pointer)==0)
              {
                FROMA="1999-01-01 00:00:00"
                Q=paste0("'",unlist(MPIDS),"-",IsDerivedResult[1,"EQUIPMENTID"],"'",collapse=" OR ROWKEY=")
                PossibleError <- tryCatch((df=dbGetQuery(con,paste("select ROWKEY,EQUIPMENTID,MODELPARAMETERID,RECORDEDTIMESTAMP,UPDATEDTIMESTAMP,VALUE from LATESTPARAMETERVALUE where ROWKEY =",Q,sep=""))),
                                          error=function(e) e
                ) 
                if(!inherits(PossibleError, "error") && nrow(df)==length(MPIDS))
                {
                  Cdate=Sys.Date();
                  ResultSet=data.frame()
                  for(MID in 1:length(MPIDS))
                  {
                    RSdf=data.frame();
                    Q=paste0("'",MPIDS[MID],"-",IsDerivedResult[1,"EQUIPMENTID"],"-%'",collapse=" OR ROWKEY like ");
                    MinMPID=paste(MPIDS[MID],"-",IsDerivedResult[1,"EQUIPMENTID"],sep="");
                    MaxMPID=paste(MPIDS[MID],"-",as.integer(IsDerivedResult[1,"EQUIPMENTID"])+1,sep="");
                    PossibleError <- tryCatch((RSdf=rimpala.query(paste("select ROWKEY,EQUIPMENTID,MODELPARAMETERID,RECORDEDTIMESTAMP,UPDATEDTIMESTAMP,VALUE from RHISTORYPARAMETERVALUE  where (rowkey>'",MinMPID,"' and rowkey<'",MaxMPID,"') and EQUIPMENTID='",IsDerivedResult[1,"EQUIPMENTID"],"' and MODELPARAMETERID='",MPIDS[MID],"'",sep=""))),
                                              error=function(e) e)
                    if(nrow(RSdf)>0)
                    {
                      colnames(RSdf)=c("ROWKEY","EQUIPMENTID","MODELPARAMETERID","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP","VALUE");
                      ResultSet=rbind(ResultSet,RSdf);  
                    }else{TFlag=1;rimpala.close();dbDisconnect(con);return();}
                    
                  }
                  
                  if(!inherits(PossibleError, "error"))
                  {
                    colnames(ResultSet)=c("ROWKEY","EQUIPMENTID","MODELPARAMETERID","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP","VALUE")
                    LASTV[1:length(MPIDS)]=0
                    ResultSet[,"RECORDEDTIMESTAMP"]=gsub("\\.0","",ResultSet[,"RECORDEDTIMESTAMP"])
                    TST=ResultSet[nrow(ResultSet),"UPDATEDTIMESTAMP"]
                  }else{
                    TFlag=1;rimpala.close();dbDisconnect(con);return();
                  }
                }else{
                  TFlag=1;rimpala.close();dbDisconnect(con);return();
                }
                if(length(ResultSet)==0){TFlag=1;rimpala.close();dbDisconnect(con);return();}
                
              }else{
                Cdate=Sys.Date();
                #Pointer[,"POINTER"]="2016-01-01 07:30:00"
                FROM=Pointer[,"POINTER"];
                PossibleError <- tryCatch((FROMA=dbGetQuery(con,paste("select UPDATEDTIMESTAMP from LATESTPARAMETERVALUE where ROWKEY ='",IsDerivedResult[1,"MODELPARAMETERID"],"-",IsDerivedResult[1,"EQUIPMENTID"],"'",sep=""))[[1]]),
                                          error=function(e) e)
                if(is.na(Pointer[1,1])){FROMA=paste(as.character(cut(Sys.Date(), "month"))," 00:00:00",sep="");}
                if(!inherits(PossibleError, "error"))
                {
                  Q=paste0("'",unlist(MPIDS),"-",IsDerivedResult[1,"EQUIPMENTID"],"'",collapse=" OR ROWKEY=")
                  PossibleError <- tryCatch((df=dbGetQuery(con,paste("select ROWKEY,EQUIPMENTID,MODELPARAMETERID,RECORDEDTIMESTAMP,UPDATEDTIMESTAMP,VALUE from LATESTPARAMETERVALUE where ROWKEY =",Q," and recordedtimestamp >='",FROM,"'",sep=""))),
                                            error=function(e) e
                  )
                  if(!inherits(PossibleError, "error") && nrow(df)==length(MPIDS))
                  {
                    #MTS=max(df[,"UPDATEDTIMESTAMP"])
                    MINRTS=min(df[,"RECORDEDTIMESTAMP"])
                    TST=min(MINRTS,as.character(Cdate))
                  }else{
                    TFlag=1;rimpala.close();dbDisconnect(con);return();
                  }
                  #if(TST>FROMA && FROM<=FROMA)
                  #if(TFlag!=1 && TST>FROM)
                  if(TFlag!=1)
                  {
                    #STime=Sys.time()
                    ResultSet=data.frame()
                    for(MID in 1:length(MPIDS))
                    {
                      RSdf=data.frame();
                      Q=paste0("'",MPIDS[MID],"-",IsDerivedResult[1,"EQUIPMENTID"],"-%'",collapse=" OR ROWKEY like ");
                      MinMPID=paste(MPIDS[MID],"-",IsDerivedResult[1,"EQUIPMENTID"],sep="");
                      MaxMPID=paste(MPIDS[MID],"-",as.integer(IsDerivedResult[1,"EQUIPMENTID"])+1,sep="");
                      PossibleError <- tryCatch((RSdf=rimpala.query(paste("select ROWKEY,EQUIPMENTID,MODELPARAMETERID,RECORDEDTIMESTAMP,UPDATEDTIMESTAMP,VALUE from RHISTORYPARAMETERVALUE  where (rowkey>'",MinMPID,"' and rowkey<'",MaxMPID,"') and EQUIPMENTID='",IsDerivedResult[1,"EQUIPMENTID"],"' and MODELPARAMETERID='",MPIDS[MID],"' and updatedtimestamp >='",FROM,"'",sep=""))),
                                                error=function(e) e)
                      if(nrow(RSdf)>0)
                      {
                        colnames(RSdf)=c("ROWKEY","EQUIPMENTID","MODELPARAMETERID","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP","VALUE");
                        ResultSet=rbind(ResultSet,RSdf);  
                      }else{TFlag=1;rimpala.close();dbDisconnect(con);return();}
                      
                    } 
                    #et=difftime(Sys.time(),STime,unit="sec")
                    if(!inherits(PossibleError, "error"))
                    {
                      #info(logger,paste("nrow,",nrow(ResultSet),", Query took ,",et,sep=" "))
                      colnames(ResultSet)=c("ROWKEY","EQUIPMENTID","MODELPARAMETERID","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP","VALUE")  
                      #                 if(length(FROMA)>0 && FROMA>FROM)
                      #                 {
                      #                 Indx=ResultSet[,"RECORDEDTIMESTAMP"]>FROMA;
                      #                 a=ResultSet[!Indx,c(3,6)]
                      #                 a=a[order(a[,1]),]
                      #                 LASTV=a[c(match(MPIDS,a[,"MODELPARAMETERID"])),"VALUE"]
                      #                 LASTV[is.na(LASTV)]=0;
                      #                 ResultSet=ResultSet[Indx,];
                      #                 }
                      ResultSet[,"RECORDEDTIMESTAMP"]=gsub("\\.0","",ResultSet[,"RECORDEDTIMESTAMP"]);
                      Fts=1;
                    }else{
                      TFlag=1;
                    }               
                    
                  }else{
                    TFlag=1;rimpala.close();dbDisconnect(con);return();
                  }
                  if(TFlag!=1 && length(ResultSet)==0)
                  {
                    TFlag=1;rimpala.close();dbDisconnect(con);return();
                  }
                  
                }else{
                  TFlag=1;rimpala.close();dbDisconnect(con);return();
                }
                
              }  
              rimpala.close();
              if(TFlag!=1)
              {  
                if(!is.null(Shift))
                { SETIME=ShiftSETIME(Shift,IsDerivedResult[1,"EQUIPMENTID"]);
                if(length(SETIME)>0){
                  Shifttime=strftime(SETIME[1],"%H:%M:%S")
                  if(strftime(Sys.time(),"%H:%M:%S")>Shifttime)
                  {STDATE=paste(Sys.Date(),Shifttime,sep=" ");
                  }else{STDATE=paste(Sys.Date()-1,Shifttime,sep=" ");
                  }
                  SShifttime=as.POSIXlt(SETIME[1])+3660;
                  SShifttime=strftime(SShifttime,"%H:%M:%S")
                }
                
                
                }else{SShifttime="00:00:00"
                STDATE=paste(Sys.Date(),"00:00:00",sep=" ");}
                MTS=c();
                for(M in 1:length(MPIDS))
                {
                  RS=ResultSet[which(ResultSet[,"MODELPARAMETERID"]==MPIDS[M]),]
                  aggdf=data.frame()
                  if(nrow(RS)>0 && TFlag!=1)
                  {
                    RS=RS[RS[,"UPDATEDTIMESTAMP"]<STDATE,];
                    MTS[M]=RS[nrow(RS),"UPDATEDTIMESTAMP"];
                    Months=strftime(RS$UPDATEDTIMESTAMP,"%Y-%m")
                    TM=unique(Months)
                    FROMA=strftime(FROMA,"%Y-%m-%d")
                    if(length(TM)>1)
                    {  
                      for(MO in 1:length(TM))
                      {
                        RS1=RS[which(TM[MO]==Months),]
                        days=strftime(RS1$UPDATEDTIMESTAMP,"%Y-%m-%d")
                        if(length(days)>0)
                        {
                          for(MM in 1:length(days))
                          { 
                            if(length(FROMA)==0 || days[MM]>=FROMA)
                            {
                              #AGDF=aggregate(RS1$VALUE[1:MM], by=list(days[1:MM]),Fname)
                              Res=eval(parse(text=paste0(Fname,"(RS1$VALUE[1:MM])")))
                              AGDF=cbind(days[MM],Res)
                              colnames(AGDF)[1:2]=c("Day",MPIDS[M])
                              aggdf=rbind(aggdf,AGDF)
                              #colnames(aggdf)[1:2]=c("Day",MPIDS[M])  
                            }
                            
                          }
                        }
                      }
                      
                    }else{
                      days=strftime(RS$UPDATEDTIMESTAMP,"%Y-%m-%d")
                      if(length(days))
                      {
                        for(MM in 1:length(days))
                        { 
                          if(length(FROMA)==0 || days[MM]>=FROMA)
                          {
                            Res=eval(parse(text=paste0(Fname,"(RS$VALUE[1:MM])")))
                            AGDF=cbind(days[MM],Res)
                            colnames(AGDF)[1:2]=c("Day",MPIDS[M])
                            aggdf=rbind(aggdf,AGDF)
                            #colnames(aggdf)[1:2]=c("Day",MPIDS[M])
                          }
                        }
                      }
                    }
                    #colnames(aggdf)[1:2]=c("Month",MPIDS[M])
                    if(M>1){
                      df=merge(df,aggdf, by=c("Day"),all.x = TRUE);
                    }else{
                      df=aggdf;
                    }
                  }else{ TFlag=1;rimpala.close();dbDisconnect(con);return();}
                }  
                if(((ncol(df)-1)==length(MPIDS)) && TFlag!=1)
                {
                  DPH=data.frame()
                  Res=c()
                  Index=complete.cases(df)
                  if(length(Index)>1 && any(Index))
                  {df=df[Index,]}
                  if(nrow(df)>0){Res=ExEVALHM(df,ExpressionM,MPIDS);}
                  #Res=Res[Index]
                  if(nrow(df)>0 && length(Res)>0)
                  {
                    UTS=paste(df[,"Day"],SShifttime,sep=" ");
                    TST=paste(strftime(UTS[length(UTS)],"%Y-%m"),"-01 ",SShifttime,sep="");
                    #TST=RTS[length(RTS)];
                    #                 MDate=strftime(min(MTS),"%Y-%m-%d");
                    #                 MTS=paste(MDate,"23:59:59")
                    #                 if(length(RTS)>1)
                    #                 {
                    #                   if(nrow(df)>1)
                    #                   { 
                    #                     UTS=c();
                    #                     for(U in 1:nrow(df))
                    #                     {
                    #                       UTS[U]=max(ResultSet[grep(df[U,'Month'],ResultSet[,"UPDATEDTIMESTAMP"]),"UPDATEDTIMESTAMP"])
                    #                     }
                    #                     
                    #                   }else{UTS=c(RTS[1:(length(RTS)-1)],MTS);}
                    #                   
                    #                 }else{UTS=MTS;}
                    RKH=suppressWarnings(paste(IsDerivedResult[1,"MODELPARAMETERID"],IsDerivedResult[1,"EQUIPMENTID"],UTS,sep="-"));
                    DPH=data.frame(cbind(RKH,IsDerivedResult[1,"MODELPARAMETERID"],IsDerivedResult[1,"EQUIPMENTID"],Res,UTS,UTS),stringsAsFactors=FALSE)
                  }else{TFlag=1;rimpala.close();dbDisconnect(con);return();}
                }else{TFlag=1;rimpala.close();dbDisconnect(con);return();}
              }
            },warning = function(war){ warn(logger,paste(ExpressionM,"for",IsDerivedResult[1,"MODELPARAMETERID"],"&",IsDerivedResult[1,"EQUIPMENTID"],war,sep=" "));war;}, 
            error = function(err) { error(logger,paste(ExpressionM,"for",IsDerivedResult[1,"MODELPARAMETERID"],"&",IsDerivedResult[1,"EQUIPMENTID"],err,sep=" "));err; })
            rimpala.close();
            if(TFlag!=1 && !inherits(PossibleError, "error") && !inherits(PossibleError, "warn") && nrow(DPH)>0)  
            {
              Flag=WriteF(DPH,paste(IsDerivedResult[1,"MODELPARAMETERID"],IsDerivedResult[1,"EQUIPMENTID"],sep="-"))
              if(Flag==1){suppressWarnings(dbGetQuery(con,paste0("insert into POINTER (MODELPARAMETERID,EQUIPMENTID,POINTER) values (",IsDerivedResult[1,"MODELPARAMETERID"],",",IsDerivedResult[1,"EQUIPMENTID"],",'",TST,"') ON DUPLICATE KEY UPDATE POINTER='",TST,"';")))}
            }
            dbDisconnect(con)        
          }
        }
        
        MovingMTY<-function(ExpressionM,Fname,Shift=NULL,Condition=NULL,IsDerivedResult,Pointer,logger,LFLAG)
        {
          TFlag=0;
          MPIDS=c();
          LMPIDS=c();
          if(length(ExpressionM)>0)
          {
            rimpala.init(libs="/opt/cloudera/parcels/IMPALA/lib/impala/lib/")
            rimpala.connect("x.x.x.x","21050")
            con <- dbConnect(dbDriver("MySQL"),user=userDB,password=passDB,dbname=DBName,host=DBHost,port=3306)
            cm <- gregexpr("CM[0-9]*", ExpressionM, perl=TRUE)
            if(any(cm[[1]]!=-1))
            {
              CMMPIDS=sort(unique(unlist(regmatches(ExpressionM,cm))));
              CMMPIDS=gsub("CM","",CMMPIDS);
              CMMPVAL=suppressWarnings(dbGetQuery(con,paste0("SELECT MODELPARAMETERID,PARAMETERVALUE FROM COMMISSIONINGPOINT where EQUIPMENTID=",IsDerivedResult[1,"EQUIPMENTID"]," and MODELPARAMETERID IN (",paste(CMMPIDS,collapse=","),");")))
              if(nrow(CMMPVAL)>0)
              {
                for(c in 1:nrow(CMMPVAL))
                {
                  ExpressionM=gsub(paste("CM",CMMPVAL[c,1],sep=""),CMMPVAL[c,2],ExpressionM)
                }
                ExpressionM=gsub("CM","",ExpressionM);
                
              }else{
                ExpressionM=gsub("CM","HM",ExpressionM);
              }
            }
            hm <- gregexpr("HM[0-9]*", ExpressionM, perl=TRUE)
            if(any(hm[[1]]!=-1))
            {
              HMPIDS=sort(unique(unlist(regmatches(ExpressionM,hm))))
              HMPIDS=gsub("HM","",HMPIDS)
              MPIDS=c(MPIDS,HMPIDS)
            }
            lm <- gregexpr("LM[0-9]*", ExpressionM, perl=TRUE)
            if(any(lm[[1]]!=-1))
            {
              LMPIDS=sort(unique(unlist(regmatches(ExpressionM,lm))));
              LMPIDS=gsub("LM","",LMPIDS);
              MPIDS=c(MPIDS,LMPIDS);
              pm<- gregexpr("@[A-Z]*", ExpressionM, perl=TRUE);
              pm=sort(unique(unlist(regmatches(ExpressionM,pm))));
              pm=gsub("@","",pm);
              if(pm=="STW")
              {
                pm=' Std. Weight';
              }else{if(pm=="ICT"){pm='Ideal Cycle time';}else{pm='Total Cavities';}}
              
            }
            LASTV=c()
            Fts=0;
            OffMPIDS=suppressWarnings(dbGetQuery(con,paste0("SELECT ANALOGMODELPARAMETERID FROM EQUIPMENTPARAMETER WHERE ISONSCREEN=1 AND EQUIPMENTID=",IsDerivedResult[1,"EQUIPMENTID"]," AND  ANALOGMODELPARAMETERID IN (",paste(MPIDS,collapse=","),");")))  
            if(LFLAG==0 && nrow(OffMPIDS)>0){
              Q=paste0("'",unlist(OffMPIDS),"-",IsDerivedResult[1,"EQUIPMENTID"],"'",collapse=" OR ROWKEY=")
              PossibleError <- tryCatch((df=dbGetQuery(con,paste("select * from LATESTPARAMETERVALUE where ROWKEY =",Q,sep=""))),
                                        error=function(e) e
              )
              colnames(df)=c("ROWKEY","MODELPARAMETERID","EQUIPMENTID","VALUE","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP")
              for(Oind in 1:nrow(OffMPIDS))
              {
                val=df[which(OffMPIDS[Oind]==df[,"MODELPARAMETERID"]),"VALUE"]
                ExpressionM=gsub(paste("HM",OffMPIDS[Oind],sep=""),val,ExpressionM)
                MPIDS=MPIDS[is.na(match(MPIDS,OffMPIDS[Oind]))]
              }
            }
            PossibleError <- tryCatch({
              if(nrow(Pointer)==0)
              {
                FROMA="1999-01-01 00:00:00"
                Q=paste0("'",unlist(MPIDS),"-",IsDerivedResult[1,"EQUIPMENTID"],"'",collapse=" OR ROWKEY=")
                PossibleError <- tryCatch((df=dbGetQuery(con,paste("select ROWKEY,EQUIPMENTID,MODELPARAMETERID,RECORDEDTIMESTAMP,UPDATEDTIMESTAMP,VALUE from LATESTPARAMETERVALUE where ROWKEY =",Q,sep=""))),
                                          error=function(e) e
                ) 
                if(!inherits(PossibleError, "error") && nrow(df)==length(MPIDS))
                {
                  Cdatetime=Sys.time()
                  if(!is.null(Shift))
                  { SETIME=ShiftSETIME(Shift,IsDerivedResult[1,"EQUIPMENTID"]);
                  if(length(SETIME)>0){Shifttime=strftime(SETIME[1],"%H:%M:%S");}
                  }else{
                    Shifttime="00:00:00";
                    SShifttime="00:00:00";
                  }
                  if(strftime(Cdatetime,"%H:%M:%S")>Shifttime)
                  {STDATE=paste(as.Date(Cdatetime),Shifttime,sep=" ");
                  }else{STDATE=paste(as.Date(Cdatetime)-1,Shifttime,sep=" ");}
                  FROM=paste(as.Date(STDATE)-30,Shifttime,sep=" ");
                  SShifttime=as.POSIXlt(SETIME[1])+3660;
                  SShifttime=strftime(SShifttime,"%H:%M:%S")
                  ResultSet=data.frame()
                  for(MID in 1:length(MPIDS))
                  {
                    RSdf=data.frame();
                    Q=paste0("'",MPIDS[MID],"-",IsDerivedResult[1,"EQUIPMENTID"],"-%'",collapse=" OR ROWKEY like ");
                    MinMPID=paste(MPIDS[MID],"-",IsDerivedResult[1,"EQUIPMENTID"],sep="");
                    MaxMPID=paste(MPIDS[MID],"-",as.integer(IsDerivedResult[1,"EQUIPMENTID"])+1,sep="");
                    PossibleError <- tryCatch((RSdf=rimpala.query(paste("select ROWKEY,EQUIPMENTID,MODELPARAMETERID,RECORDEDTIMESTAMP,UPDATEDTIMESTAMP,VALUE from RHISTORYPARAMETERVALUE  where (rowkey>'",MinMPID,"' and rowkey<'",MaxMPID,"') and EQUIPMENTID='",IsDerivedResult[1,"EQUIPMENTID"],"' and MODELPARAMETERID='",MPIDS[MID],"' and updatedtimestamp >='",FROM,"'",sep=""))),
                                              error=function(e) e)
                    if(nrow(RSdf)>0)
                    {
                      colnames(RSdf)=c("ROWKEY","EQUIPMENTID","MODELPARAMETERID","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP","VALUE");
                      ResultSet=rbind(ResultSet,RSdf);  
                    }else{TFlag=1;rimpala.close();dbDisconnect(con);return();}
                    
                  } 
                  if(!inherits(PossibleError, "error"))
                  {
                    colnames(ResultSet)=c("ROWKEY","EQUIPMENTID","MODELPARAMETERID","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP","VALUE")
                    LASTV[1:length(MPIDS)]=0
                    ResultSet[,"RECORDEDTIMESTAMP"]=gsub("\\.0","",ResultSet[,"RECORDEDTIMESTAMP"])
                    TST=ResultSet[nrow(ResultSet),"UPDATEDTIMESTAMP"]
                  }else{
                    TFlag=1;rimpala.close();dbDisconnect(con);return();
                  }
                }else{
                  TFlag=1;rimpala.close();dbDisconnect(con);return();
                }
                if(length(ResultSet)==0||nrow(ResultSet)==0){TFlag=1;rimpala.close();dbDisconnect(con);return();}
                
                
              }else{
                Cdate=Sys.Date();
                FROM=Pointer[,"POINTER"]
                PossibleError <- tryCatch((FROMA=dbGetQuery(con,paste("select UPDATEDTIMESTAMP from LATESTPARAMETERVALUE where ROWKEY ='",IsDerivedResult[1,"MODELPARAMETERID"],"-",IsDerivedResult[1,"EQUIPMENTID"],"'",sep=""))[[1]]),
                                          error=function(e) e)
                
                if(!inherits(PossibleError, "error"))
                {
                  Q=paste0("'",unlist(MPIDS),"-",IsDerivedResult[1,"EQUIPMENTID"],"'",collapse=" OR ROWKEY=")
                  PossibleError <- tryCatch((df=dbGetQuery(con,paste("select ROWKEY,EQUIPMENTID,MODELPARAMETERID,RECORDEDTIMESTAMP,UPDATEDTIMESTAMP,VALUE from LATESTPARAMETERVALUE where ROWKEY =",Q," and recordedtimestamp >='",FROM,"'",sep=""))),
                                            error=function(e) e
                  )
                  if(!inherits(PossibleError, "error") && nrow(df)==length(MPIDS))
                  {
                    
                    #Cdatetime=min(df[,"UPDATEDTIMESTAMP"]);
                    Cdatetime=Sys.time()
                    if(!is.null(Shift))
                    { SETIME=ShiftSETIME(Shift,IsDerivedResult[1,"EQUIPMENTID"]);
                    if(length(SETIME)>0){Shifttime=strftime(SETIME[1],"%H:%M:%S");}
                    
                    }else{
                      Shifttime="00:00:00";
                      SShifttime="00:00:00";
                    }
                    if(strftime(Cdatetime,"%H:%M:%S")>Shifttime)
                    {STDATE=paste(as.Date(Cdatetime),Shifttime,sep=" ");
                    }else{STDATE=paste(as.Date(Cdatetime)-1,Shifttime,sep=" ");  }
                    FROM=paste(as.Date(STDATE)-30,Shifttime,sep=" ");
                    SShifttime=as.POSIXlt(SETIME[1])+3660;
                    SShifttime=strftime(SShifttime,"%H:%M:%S")
                    
                  }else{
                    TFlag=1;rimpala.close();dbDisconnect(con);return();
                  }
                  
                  if(TFlag!=1)
                  {
                    ResultSet=data.frame()
                    for(MID in 1:length(MPIDS))
                    {
                      RSdf=data.frame();
                      Q=paste0("'",MPIDS[MID],"-",IsDerivedResult[1,"EQUIPMENTID"],"-%'",collapse=" OR ROWKEY like ");
                      MinMPID=paste(MPIDS[MID],"-",IsDerivedResult[1,"EQUIPMENTID"],sep="");
                      MaxMPID=paste(MPIDS[MID],"-",as.integer(IsDerivedResult[1,"EQUIPMENTID"])+1,sep="");
                      PossibleError <- tryCatch((RSdf=rimpala.query(paste("select ROWKEY,EQUIPMENTID,MODELPARAMETERID,RECORDEDTIMESTAMP,UPDATEDTIMESTAMP,VALUE from RHISTORYPARAMETERVALUE  where (rowkey>'",MinMPID,"' and rowkey<'",MaxMPID,"') and EQUIPMENTID='",IsDerivedResult[1,"EQUIPMENTID"],"' and MODELPARAMETERID='",MPIDS[MID],"' and updatedtimestamp >='",FROM,"'",sep=""))),
                                                error=function(e) e)
                      if(nrow(RSdf)>0)
                      {
                        colnames(RSdf)=c("ROWKEY","EQUIPMENTID","MODELPARAMETERID","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP","VALUE");
                        ResultSet=rbind(ResultSet,RSdf);  
                      }else{TFlag=1;rimpala.close();dbDisconnect(con);return();}
                      
                    } 
                    if(!inherits(PossibleError, "error"))
                    {
                      colnames(ResultSet)=c("ROWKEY","EQUIPMENTID","MODELPARAMETERID","RECORDEDTIMESTAMP","UPDATEDTIMESTAMP","VALUE");  
                      ResultSet[,"RECORDEDTIMESTAMP"]=gsub("\\.0","",ResultSet[,"RECORDEDTIMESTAMP"]);
                      Fts=1;
                    }else{
                      TFlag=1;
                    }               
                    
                  }else{
                    TFlag=1;rimpala.close();dbDisconnect(con);return();
                  }
                  if(TFlag!=1 && length(ResultSet)==0)
                  {
                    TFlag=1;rimpala.close();dbDisconnect(con);return();
                  }
                  
                }else{
                  TFlag=1;rimpala.close();dbDisconnect(con);return();
                }
                
              }  
              rimpala.close();
              if(TFlag!=1)
              {  
                if(length(LMPIDS)>0)
                {
                  for(LID in 1:length(LMPIDS))
                  {
                    ATTID=suppressWarnings(dbGetQuery(con,paste0("SELECT AttributeID FROM superaxis.ATTRIBUTEMASTER where MODELPARAMETERID=",LMPIDS[LID],"and DESCRIPTION='",pm,"' and (EQUIPMENTID IS NULL OR EQUIPMENTID=",IsDerivedResult[1,'EQUIPMENTID'],");")));
                    IX=which(LMPIDS[LID]==ResultSet[,"MODELPARAMETERID"])
                    SubSet=ResultSet[IX,]
                    if(nrow(SubSet)>0)
                    {
                      VAL=unique(ResultSet[IX,"VALUE"])
                      VALUES=paste(unlist(VAL),collapse=",")
                      #MDF=suppressWarnings(dbGetQuery(con,paste0("SELECT ModelParameterValue,Value FROM ATTRIBUTEVALUE WHERE AttributeMasterId  IN (SELECT AttributeId FROM ATTRIBUTEMASTER WHERE MODELPARAMETERID=8001 AND DESCRIPTION =' Std. Weight') AND ModelParameterValue IN (",VALUES,")")));
                      MDF=suppressWarnings(dbGetQuery(con,paste0("SELECT ModelParameterValue,Value FROM ATTRIBUTEVALUE WHERE AttributeMasterId=",ATTID[[1]]," AND ModelParameterValue IN (",VALUES,")")));
                      for(V in 1:nrow(MDF))
                      {
                        SubSet[which(MDF[V,"ModelParameterValue"]==SubSet[,"VALUE"]),"VALUE"]=as.numeric(MDF[V,"Value"])
                      }
                      NZInd=(VAL!=0);
                      ResultSet[IX,"VALUE"]=SubSet[,"VALUE"]
                      if(nrow(MDF)!=sum(NZInd))
                      {
                        VAL1=VAL[NZInd]
                        if(length(VAL1)>0)
                        {
                          RVAL=VAL1[is.na(match(VAL[NZInd],MDF[,"ModelParameterValue"]))]
                          ind=c()
                          for(VV in 1:length(RVAL))
                          {
                            kind=which(ResultSet[IX,"VALUE"]==RVAL[VV])
                            ind=c(ind,kind)
                          }
                          ResultSet=ResultSet[-IX[ind],]
                          
                        }
                        
                      }
                      
                    }else{TFlag=1;rimpala.close();dbDisconnect(con);return();} 
                  }
                }
                MTS=c();
                for(M in 1:length(MPIDS))
                {
                  RS=ResultSet[which(ResultSet[,"MODELPARAMETERID"]==MPIDS[M]),]
                  aggdf=data.frame()
                  if(nrow(RS)>0 && TFlag!=1)
                  {
                    RS=RS[RS[,"UPDATEDTIMESTAMP"]<STDATE,];
                    MTS=rep(as.character(as.Date(STDATE)-1),nrow(RS));
                    aggdf=aggregate(RS$VALUE, by=list(MTS),Fname)
                    colnames(aggdf)[1:2]=c("Day",MPIDS[M])
                    if(M>1){
                      df=merge(df, aggdf, by=c("Day"),all.x = TRUE)
                    }else{df=aggdf;}
                  }else{ TFlag=1;rimpala.close();dbDisconnect(con);return();}
                }  
                if(((ncol(df)-1)==length(MPIDS)) && TFlag!=1)
                {
                  DPH=data.frame()
                  Res=c()
                  Index=complete.cases(df)
                  if(length(Index)>1 && any(Index))
                  {df=df[Index,]}
                  if(nrow(df)>0){Res=ExEVALHM(df,ExpressionM,MPIDS);}
                  if(nrow(df)>0 && length(Res)>0)
                  {
                    UTS=paste(df[nrow(df),"Day"],SShifttime,sep=" ");
                    TST=STDATE;
                    RKH=suppressWarnings(paste(IsDerivedResult[1,"MODELPARAMETERID"],IsDerivedResult[1,"EQUIPMENTID"],UTS,sep="-"));
                    DPH=data.frame(cbind(RKH,IsDerivedResult[1,"MODELPARAMETERID"],IsDerivedResult[1,"EQUIPMENTID"],Res,UTS,UTS),stringsAsFactors=FALSE)
                  }else{TFlag=1;rimpala.close();dbDisconnect(con);return();}
                }else{TFlag=1;rimpala.close();dbDisconnect(con);return();}
              }
            },warning = function(war){ warn(logger,paste(ExpressionM,"for",IsDerivedResult[1,"MODELPARAMETERID"],"&",IsDerivedResult[1,"EQUIPMENTID"],war,sep=" "));war;}, 
            error = function(err) { error(logger,paste(ExpressionM,"for",IsDerivedResult[1,"MODELPARAMETERID"],"&",IsDerivedResult[1,"EQUIPMENTID"],err,sep=" "));err; })
            
            rimpala.close();
            if(TFlag!=1 && !inherits(PossibleError, "error") && !inherits(PossibleError, "warn") && nrow(DPH)>0)  
            {
              Flag=WriteF(DPH,paste(IsDerivedResult[1,"MODELPARAMETERID"],IsDerivedResult[1,"EQUIPMENTID"],sep="-"))
              if(Flag==1){suppressWarnings(dbGetQuery(con,paste0("insert into POINTER (MODELPARAMETERID,EQUIPMENTID,POINTER) values (",IsDerivedResult[1,"MODELPARAMETERID"],",",IsDerivedResult[1,"EQUIPMENTID"],",'",TST,"') ON DUPLICATE KEY UPDATE POINTER='",TST,"';")))}
            }
            dbDisconnect(con)        
          }
        }
