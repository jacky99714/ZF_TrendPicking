{ZF VCP 強勢}
{
  條件 1：均線多頭排列 close > MA50 > MA150 > MA200
  條件 2：MA200 趨勢向上（今日 MA200 > 20日前 MA200）

  注意：打敗大盤條件無法在 XQ 指標中實現，需自行判斷
  均線皆使用收盤價計算
}

input: MA50Period(50, "MA50 週期");
input: MA150Period(150, "MA150 週期");
input: MA200Period(200, "MA200 週期");
input: SlopeLookback(20, "MA200 斜率回看天數");

variable: vMA50(0);
variable: vMA150(0);
variable: vMA200(0);
variable: vMA200Prev(0);
variable: condAlignment(false);
variable: condMA200Up(false);
variable: isVCPStrong(false);

{ 均線計算 }
vMA50 = Average(Close, MA50Period);
vMA150 = Average(Close, MA150Period);
vMA200 = Average(Close, MA200Period);
vMA200Prev = Average(Close, MA200Period)[SlopeLookback];

{ 條件 1：均線多頭排列 }
condAlignment = Close > vMA50 and vMA50 > vMA150 and vMA150 > vMA200;

{ 條件 2：MA200 趨勢向上 }
condMA200Up = vMA200 > vMA200Prev;

{ 綜合判定 }
isVCPStrong = condAlignment and condMA200Up;

{ 繪製均線 }
Plot1(vMA50, "MA50");
Plot2(vMA150, "MA150");
Plot3(vMA200, "MA200");
SetPlotColor(1, Orange);
SetPlotColor(2, Blue);
SetPlotColor(3, Red);
SetPlotWidth(3, 2);

{ 信號標記 }
if isVCPStrong then
  DrawIcon(1, Low, "強");
