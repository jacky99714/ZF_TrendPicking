{ZF 三線開花}
{
  條件 1：三線排列 close > MA8 > MA21 > MA55
  條件 2：創 55 日收盤價新高（注意：用收盤價，不是最高價）
}

input: MA8Period(8, "MA8 週期");
input: MA21Period(21, "MA21 週期");
input: MA55Period(55, "MA55 週期");
input: HighPeriod(55, "收盤新高天數");

variable: vMA8(0);
variable: vMA21(0);
variable: vMA55(0);
variable: vHigh55D(0);
variable: condAlignment(false);
variable: condNewHigh(false);
variable: isSanxian(false);

{ 均線計算 }
vMA8 = Average(Close, MA8Period);
vMA21 = Average(Close, MA21Period);
vMA55 = Average(Close, MA55Period);

{ 55 日收盤價最高（注意用 Close 不是 High） }
vHigh55D = Highest(Close, HighPeriod);

{ 條件 1：三線開花排列 }
condAlignment = Close > vMA8 and vMA8 > vMA21 and vMA21 > vMA55;

{ 條件 2：55 日收盤新高 }
condNewHigh = Close >= vHigh55D;

{ 綜合判定 }
isSanxian = condAlignment and condNewHigh;

{ 繪製均線 }
Plot1(vMA8, "MA8");
Plot2(vMA21, "MA21");
Plot3(vMA55, "MA55");
SetPlotColor(1, Cyan);
SetPlotColor(2, Blue);
SetPlotColor(3, Magenta);
SetPlotWidth(3, 2);

{ 繪製 55 日收盤高點線 }
Plot4(vHigh55D, "55日高點");
SetPlotColor(4, DarkGray);

{ 信號標記 }
if isSanxian then
  DrawIcon(1, Low, "●");
