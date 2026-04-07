{ZF VCP 新高}
{
  條件 1：近 5 日最高價 接近 260 日（52 週）最高價，差距 ≤ 1%

  注意：此指標使用「最高價」計算，不是收盤價
  注意：打敗大盤條件無法在 XQ 指標中實現，需自行判斷
}

input: High5Period(5, "近期高點天數");
input: High52WPeriod(260, "52 週高點天數");
input: Tolerance(0.01, "新高容差 (1%)");

variable: vHigh5D(0);
variable: vHigh260D(0);
variable: vGapPct(0);
variable: isVCPNewHigh(false);

{ 高低點計算（使用最高價） }
vHigh5D = Highest(High, High5Period);
vHigh260D = Highest(High, High52WPeriod);

{ 條件：接近 52 週新高 }
if vHigh260D > 0 then begin
  vGapPct = AbsValue(vHigh5D / vHigh260D - 1);
  isVCPNewHigh = vGapPct <= Tolerance;
end;

{ 繪製 52 週高點線 }
Plot1(vHigh260D, "52週高點");
SetPlotColor(1, Yellow);
SetPlotWidth(1, 2);

{ 繪製容差區間 }
Plot2(vHigh260D * (1 + Tolerance), "容差上限");
Plot3(vHigh260D * (1 - Tolerance), "容差下限");
SetPlotColor(2, DarkYellow);
SetPlotColor(3, DarkYellow);

{ 信號標記 }
if isVCPNewHigh then
  DrawIcon(1, High, "高");
