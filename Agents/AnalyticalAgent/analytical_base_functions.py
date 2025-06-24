def calculate_cagr(initial_revenue: float, latest_revenue: float, no_of_years: int) -> float:
    cagr_percentage = (((latest_revenue/initial_revenue) ** (1/no_of_years))-1)*100
    return cagr_percentage

def calculate_yoy_growth(revenue_current_year: float, revenue_previous_year: float)->float:
    yoy_revenue_growth_percentage = ((revenue_current_year - revenue_previous_year)/revenue_previous_year)*100
    return yoy_revenue_growth_percentage