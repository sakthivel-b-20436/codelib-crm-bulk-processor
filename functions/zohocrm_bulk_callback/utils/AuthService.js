class AuthService {
  #DefaultKey = 'CODELIB_FAKE_KEY'

  isValidRequest = (key) => {
    return (
      key &&
      key !== this.#DefaultKey &&
      key === process.env.CODELIB_SECRET_KEY
    )
  }

  static getInstance = () => {
    return new AuthService()
  }
}
module.exports = AuthService
