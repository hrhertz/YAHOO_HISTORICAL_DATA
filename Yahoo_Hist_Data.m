%%
% Create Table in PostgreSQL 10 for TWS stock history.
%
% Matlab 2017a w/ Database Tool Box.
%
% Copyright (c) 2018 Ken Segura.
%
% Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
% The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

clear;

format bank;

pause('off');

%%
% Exchange Name.
exchageName = ('nyse');

%%
% Dates.
% DayMonthYear
startDateString = ['01012000'];
endDateString = ['28022018'];

%%
% Write Start to Error Log
fileNameA = sprintf('%s_tickers_good.txt',exchageName);
fileIDGTickers = fopen(fileNameA,'w');
fclose(fileIDGTickers);

%%
%
fileNameA = sprintf('%s_tickers.txt',exchageName);
fileIDTicker = fopen(fileNameA,'r');
    
while ~feof(fileIDTicker)
InputTextTicker = textscan(fileIDTicker,'%s[\n\r]',1);
TableTicker = InputTextTicker{1,1};
TableTickerFileName = char(TableTicker);
    

    %%
    % This calls hist_stock_data.m
    exData = hist_stock_data(startDateString,endDateString,TableTickerFileName);
    
    [exDataSize_m,exDataSize_n] = size(exData); 
    
    if exDataSize_m == 0
        disp(TableTicker);
        disp('No_Data');
        disp (exDataSize_m);
    else
        
        disp(TableTicker);
        disp ('DATA');
        disp (exDataSize_m); 
        
        fileNameB = sprintf('%s_DATA.mat',TableTickerFileName);
        save(fileNameB, 'exData');

        fileNameC = sprintf('%s_tickers_good.txt',exchageName);
        fileIDGTickers = fopen(fileNameC,'a');
        fprintf(fileIDGTickers,'%s\r\n',TableTickerFileName);
        fclose(fileIDGTickers);
        
    end;
    
disp('Paused');
n = 10;
pause(n);

clear exData;

end;

    %%close(ib);

fclose(fileIDTicker);
%
%%%%%
