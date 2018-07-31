import struct
import wallaroo

def application_setup(args):
    in_host, in_port = wallaroo.tcp_parse_input_addrs(args)[0]
    out_host, out_port = wallaroo.tcp_parse_output_addrs(args)[0]

    ab = wallaroo.ApplicationBuilder("Add2")
    ab.new_pipeline("add_2",
                    wallaroo.TCPSourceConfig(in_host, in_port, decoder))
    ab.to(add_2)
    ab.to_sink(wallaroo.TCPSinkConfig(out_host, out_port, encoder))
    return ab.build()

@wallaroo.decoder(header_length=4, length_fmt=">I")
def decoder(bs):
    return bs.decode("utf-8")

@wallaroo.computation(name="add_2")
def add_2(data):
    result = int(data) + 2
    return str(result)

@wallaroo.encoder
def encoder(data):
    # data is an int
    return data + "\n"
