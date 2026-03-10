const { chromium } = require('playwright');

(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage();
  
  // Set viewport to capture full width
  await page.setViewportSize({ width: 1400, height: 900 });
  
  // Load the HTML file
  await page.goto('file:///Users/owencoonahan/Documents/World%20Domination/Offers/Queue%20Analysis/tools/data_ingestion_diagram.html');
  
  // Wait for content to render
  await page.waitForTimeout(500);
  
  // Take full page screenshot
  await page.screenshot({ 
    path: 'diagram_screenshot.png', 
    fullPage: true 
  });
  
  console.log('Screenshot saved to diagram_screenshot.png');
  
  await browser.close();
})();
