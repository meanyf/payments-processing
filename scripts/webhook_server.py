"""Тестовый сервер для получения webhook-уведомлений."""

from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    """Выводит полученное уведомление и подтверждает доставку."""

    def do_POST(self):
        """Принимает тело POST-запроса и отвечает статусом 204."""
        body = self.rfile.read(int(self.headers["Content-Length"]))
        print("Webhook:", body.decode(), flush=True)
        self.send_response(204)
        self.end_headers()


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", 9000), Handler).serve_forever()
