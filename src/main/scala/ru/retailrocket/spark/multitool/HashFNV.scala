object HashFNV {
  val FNV_32_INIT: Int = 33554467
  val FNV_32_PRIME: Int = 0x01000193

  def hash(s: String, init: Int=FNV_32_INIT): Int = {
    var hval = init
    val bytes = s.getBytes
    for(i <- bytes) {
      hval *= FNV_32_PRIME
      hval ^= i
    }
    hval
  }
}
