from dart.web import app


if __name__ == '__main__':
    app.run(host=app.config['dart_host'], port=app.config['dart_port'], use_reloader=False)
