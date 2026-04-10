import zmq

def main():
    context = zmq.Context()

    xsub = context.socket(zmq.XSUB)
    xsub.bind("tcp://*:5557")

    xpub = context.socket(zmq.XPUB)
    xpub.bind("tcp://*:5558")

    print("[PubSub Proxy] Rodando. XSUB=5557, XPUB=5558")

    try:
        zmq.proxy(xsub, xpub)
    except KeyboardInterrupt:
        pass
    finally:
        xsub.close()
        xpub.close()
        context.term()

if __name__ == "__main__":
    main()
