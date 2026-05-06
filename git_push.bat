@echo off
cd C:\ArtisticDentalPortal
git add cache/latest/
git commit -m "Auto-update data %date%"
git push origin main