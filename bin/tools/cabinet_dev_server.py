"""Authenticated portal preview without starting unrelated scheduled workers."""
import argparse
import sys
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))

def main():
    parser=argparse.ArgumentParser(); parser.add_argument('--port',type=int,default=8790)
    args=parser.parse_args()
    from clipflow_backend.main import FastAPIPortalController
    import uvicorn
    controller=FastAPIPortalController(host='127.0.0.1',port=args.port)
    controller.bound_port=args.port
    app=controller._build_app()
    uvicorn.run(app,host='127.0.0.1',port=args.port,log_level='warning',log_config=None)

if __name__=='__main__': main()
