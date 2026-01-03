from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, timedelta


TIER_1_SEEDS = ["Ba chỉ bò", "Ba rọi heo", "Bò lúc lắc", "Bò viên", "Bạch tuộc", "Bắp bò", "Bắp heo", "Chân giò heo", "Chân gà", "Chả cá", "Cua đồng xay", "Cá basa", "Cá bạc má", "Cá chim", "Cá diêu hồng", "Cá hường", "Cá hồi", "Cá lóc", "Cá ngân", "Cá ngừ", "Cá nục", "Cá sòng", "Cá sặc", "Cá viên", "Cánh gà", "Cốt lết", "Hến", "Lòng Gà", "Mề gà", "Nghêu", "Nạc vai heo", "Nạm bò", "Râu mực", "Sò lông", "Sườn heo", "Sụn gà", "Sứa", "Thăn heo", "Thịt heo xay", "Thịt vụn bò", "Thịt đùi heo", "Tim gà", "Trứng cút", "Trứng gà", "Trứng vịt", "Tôm khô", "Tôm thẻ", "Tôm viên", "Tỏi gà", "Vịt", "Xương gà", "Xương heo", "Đuôi mực", "Đùi bò", "Đùi gà", "Đầu cá hồi", "Ếch", "Ốc bươu", "Ốc móng tay", "Ức cá basa", "Ức gà", "Bí", "Bông súng", "Bầu", "Bắp cải", "Cà chua", "Cà pháo", "Cà rốt", "Cà tím", "Cải bẹ", "Cải ngọt", "Cải ngồng", "Cải thìa", "Củ cải", "Củ dền", "Củ nghệ", "Củ sắn", "Dưa leo", "Hạt dẻ", "Khoai mỡ", "Khoai tây", "Khổ qua", "Me chua", "Mướp", "Nấm bào ngư xám", "Nấm hương", "Nấm kim châm", "Nấm linh chi", "Nấm mèo đen", "Nấm tuyết", "Nấm đông cô", "Nấm đùi gà", "Rau muống", "Rau má", "Rau mồng tơi", "Rau ngót", "Rau om", "Su su", "Trái bắp", "Xà lách", "Đậu bắp", "Đậu cove", "Đậu rồng"]
TIER_2_SEEDS = ["Bánh phồng", "Bánh tráng", "Bánh đa", "Hạt sen", "Hạt é", "Mè", "Măng", "Phổ tai", "Rong biển", "Đậu nành", "Đậu phộng", "Đậu trắng", "Đậu xanh", "Đậu đen hạt", "Đậu đỏ"]

@dataclass(frozen=True)
class Settings:
    supabase_url: str
    supabase_service_role_key: str
    source: str = "cookpad"
    locale: str = "vn"
    cutoff_days: int = 50000
    max_pages_per_keyword: int = 500

    user_agent: str = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    @property
    def cutoff_date(self) -> date:
        return date.today() - timedelta(days=self.cutoff_days)


def load_settings() -> Settings:
    supabase_url = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
    supabase_service_role_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not supabase_url or not supabase_service_role_key:
        raise RuntimeError("Missing env: SUPABASE_URL and/or SUPABASE_SERVICE_ROLE_KEY")

    source = os.environ.get("SOURCE", "cookpad").strip()
    locale = os.environ.get("LOCALE", "vn").strip()
    cutoff_days = int(os.environ.get("CUTOFF_DAYS", "30"))
    max_pages = int(os.environ.get("MAX_PAGES_PER_KEYWORD", "30"))
    return Settings(
        supabase_url=supabase_url,
        supabase_service_role_key=supabase_service_role_key,
        source=source,
        locale=locale,
        cutoff_days=cutoff_days,
        max_pages_per_keyword=max_pages,
    )

