require 'fiber'
class Fiber
  alias call resume
end
