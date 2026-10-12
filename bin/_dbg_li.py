import tempfile
from pathlib import Path
from unittest.mock import patch
from lan_bitable_template_portal.portal_auth import PortalAuthManager, LI_SHILONG_OPEN_ID

with tempfile.TemporaryDirectory() as tmp:
    root = Path(tmp)

    def fake_data_path(name):
        return str(root / name)

    with patch("lan_bitable_template_portal.portal_auth.get_data_file_path", side_effect=fake_data_path):
        m = PortalAuthManager()
        print("LI scopes:", m.scopes_for_open_id(LI_SHILONG_OPEN_ID))
        print("admin_scopes:", m._admin_scopes())
        print("LI profile:", m.account_profile(LI_SHILONG_OPEN_ID))