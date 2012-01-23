require 'socket'
module IProto

  # TODO: timeouts
  class TCPSocket < ::TCPSocket
    include IProto::ConnectionAPI

    # begin ConnectionAPI
    def send_packet(request_id, data)
      send data, 0
      body_size = recv_header request_id
      recv_body body_size
    end
    # end ConnectionAPI

    def recv_header(request_id)
      header = recv(12)
      type, body_size, recv_request_id = header.unpack('L3')
      raise UnexpectedResponse.new("Waiting response for request_id #{request_id}, but received for #{recv_request_id}") unless request_id == recv_request_id
      body_size
    end

    def recv_body(body_size)
      recv(body_size)
    end
  end
end