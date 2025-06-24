from Configurations.general_configurations import set_base_variables
set_base_variables()

from fastapi import FastAPI
import Controllers.api_agent_controller as api_agent_controller
import Controllers.scraping_agent_controller as scraping_agent_controller

app = FastAPI()

# Mount routes
app.include_router(api_agent_controller.router, prefix= "/APIAgent")
app.include_router(scraping_agent_controller.router, prefix= "/ScrappingAgent")

@app.get("/")
async def root():
    return {"message": "MarketWhisper backend running"}
