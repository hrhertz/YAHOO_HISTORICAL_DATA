%%
% Create Table and insert data in PostgreSQL 10 for yahoo stock history.
% This calls hist_stock_data.m
%
% Matlab 2017a w/ Database Tool Box.
%
% Copyright (c) 2018 Ken Segura.
%
% Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
% The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

%Clear !
clear

format bank;

pause('on');

%%
% Make sure you have JDBC loaded.
% javaclasspath('postgresql-42.2.2.jar'); 

%%
% Exchange Name.
% amex , nyse , nasdaq .
exchageName = ('amex');

%%
% Mode 0 just read files and display info.
% Mode 1 Create Tables for each ticker
% Mode 2 Delete Tables for each ticker
% Mode 3 Load data into tables.
scriptMode = 3;

%%
%
fileNameA = sprintf('%s_tickers_good.txt',exchageName);
fileIDTicker = fopen(fileNameA,'r');

%%
% Write Start to Error Log
fileIDError = fopen('Error_File_DB.txt','w');
fprintf(fileIDError,'---Start Run DB----\r\n');
fclose(fileIDError);

%%
% DB Information.
;
conn = database('<db_name>','<user>','<password>','Vendor','PostgreSQL','Server','<IP>','PortNumber',5432);

%%
%
sqlqueryCreate = 'CREATE TABLE ';
sqlqueryDrop = 'DROP TABLE ';
sqlqueryLayout = '(RecordNumber SERIAL,DateValue DATE, Open DECIMAL, High DECIMAL,  Low DECIMAL, Close DECIMAL, AdjClose DECIMAL, Volume INT)';
colNames = {'DateValue';'Open';'High';'Low';'Close';'AdjClose';'Volume'};

while ~feof(fileIDTicker)
InputTextTicker = textscan(fileIDTicker,'%s[\n\r]',1);
TableTicker = InputTextTicker{1,1};
TableTickerFileName = char(TableTicker);

fileNameB = sprintf('%s_DATA.mat',TableTickerFileName);
TableTickerMod = sprintf('%s_T',TableTickerFileName);
exData = load(fileNameB);

[exDataSize_m,exDataSize_n] = size(exData.exData.Open);

numberEntries = exDataSize_m;

        disp (TableTickerFileName); %% What ticker are we working on.
        disp (numberEntries); %% n (Number of entries) Use this for loop.

            if scriptMode == 0; %% Read out the data
     
                for ii = 1:numberEntries;

                DateVar = exData.exData.Date(ii,1);
                OpenVar = exData.exData.Open(ii,1);
                HighVar = exData.exData.High(ii,1);
                LowVar = exData.exData.Low(ii,1);
                CloseVar = exData.exData.Close(ii,1);
                AdjCloseVar = exData.exData.AdjClose(ii,1);
                VolumeVar = exData.exData.Volume(ii,1);            
                
                disp (DateVar);
                disp (OpenVar);
                disp (HighVar);
                disp (LowVar);
                disp (CloseVar);
                disp (AdjCloseVar);
                disp (VolumeVar);

                disp('Paused');
                n = 1;
                pause(n);
                
                end; %% For loop numberEntries.
                
            end; %% Script Mode 0 end.
            
            if scriptMode == 1; %% Create Talbes.

                sqlqueryTemp = [sqlqueryCreate TableTickerMod sqlqueryLayout];
                disp (sqlqueryTemp);
                %%sqlquery = strjoin(sqlqueryTemp);
 
                disp(sqlqueryTemp);
            
                curs = exec(conn,sqlqueryTemp);
                disp (curs);
            
                MessageCurs = char(curs.Message);
                
                if MessageCurs[1,1] == ('E') 
                    disp('---------------Error--------------');
                    disp(MessageCurs); 
                    disp('---------------Error--------------');

                    fileIDError = fopen('Error_Log.txt','a');
                    fprintf(fileIDError,'%s\r\n',MessageCurs);
                    fclose(fileIDError);
                             
                    %n = 1;
                    %pause(n);
                
                end; %% End Mode 1 Error.
                
            end; %% Script Mode 1 end.

            
            
            if scriptMode == 2; %% Delete Tables.

                sqlqueryTemp = [sqlqueryDrop TableTickerMod];
                
                disp(sqlqueryTemp);
            
                curs = exec(conn,sqlqueryTemp);
                disp (curs);
            
                MessageCurs = char(curs.Message);
                if MessageCurs[1,1] == ('E') 
                    disp('---------------Error--------------');
                    disp(MessageCurs); 
                    disp('---------------Error--------------');

                    fileIDError = fopen('Error_Log.txt','a');
                    fprintf(fileIDError,'%s\r\n',MessageCurs);
                    fclose(fileIDError);
                             
                    %n = 1;
                    %pause(n);
                end; %% End Mode 2 Error.
                
            end; %% Script Mode 2 end.

            
            
            if scriptMode == 3; %% Enter the data into the tables.

                for ii = 1:numberEntries;

                DateVar = exData.exData.Date(ii,1);
                OpenVar = exData.exData.Open(ii,1);
                HighVar = exData.exData.High(ii,1);
                LowVar = exData.exData.Low(ii,1);
                CloseVar = exData.exData.Close(ii,1);
                AdjCloseVar = exData.exData.AdjClose(ii,1);
                VolumeVar = exData.exData.Volume(ii,1);
                %%Ticker = exData.exData.Ticker(1,n); 1xn char ;? Same as TableTickerFileName              
                                        
                exDataDB = {DateVar, OpenVar, HighVar, LowVar, CloseVar, AdjCloseVar, VolumeVar};

                insert(conn, TableTickerMod, colNames, exDataDB);
            
                MessageCurs = char(ans);
                
                    if MessageCurs[1,1] == ('E') 
                        disp('---------------Error--------------');
                        disp(MessageCurs); 
                        disp('---------------Error--------------');

                        fileIDError = fopen('Error_File_DB.txt','a');
                        fprintf(fileIDError,'%s\r\n',MessageCurs);
                        fclose(fileIDError);
                        
                        %disp('Paused');
                        %n = 1;
                        %pause(n);
                    
                    end; %% End Mode 3 Error.
                
                disp('sleep');
                disp(ii);
                %n = 1;
                %pause(n);
                
                %java.lang.Thread.sleep(duration*1000)  % in mysec!
                java.lang.Thread.sleep(0.1*1000)  % in mysec!
                
                end; %% For loop numberEntries.  
                disp (ii);  
               
            clear numberEntries;
            clear ii;
            
            %close (conn);
            
            end; %% Script Mode 3 end.
    
end; %%End While.

%%
% Write End to Error Log
fileIDError = fopen('Error_File_EB.txt','a');
fprintf(fileIDError,'---End Run DB----\r\n');
fclose(fileIDError);

fclose(fileIDTicker);
