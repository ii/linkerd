package io.buoyant.k8s

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.http.{Request, Response}
import com.twitter.io.{Buf, Writer}
import com.twitter.util._
import io.buoyant.namer.RichActivity
import io.buoyant.test.Awaits
import org.scalatest.FunSuite
import org.scalatest.exceptions.TestFailedException
import scala.util.control.NoStackTrace

class EndpointsNamerTest extends FunSuite with Awaits {
  val WatchPath = "/api/v1/watch/"
  val SessionsPath = "namespaces/srv/endpoints/sessions"
  val NonWatchPath = "/api/v1/"
  object Rsps {
    val InitResourceVersion = "4962526"


    val Init = Buf.Utf8(
      s"""
        |{
        |  "kind": "Endpoints",
        |  "apiVersion": "v1",
        |  "metadata": {
        |    "name": "sessions",
        |    "namespace": "srv",
        |    "selfLink": "$NonWatchPath$SessionsPath",
        |    "uid": "6a698096-525e-11e5-9859-42010af01815",
        |    "resourceVersion": "4962526",
        |    "creationTimestamp": "2015-09-03T17:08:37Z"
        |  },
        |  "subsets": [
        |    {
        |      "addresses": [
        |        {
        |          "ip": "10.248.4.9",
        |          "targetRef": {
        |            "kind": "Pod",
        |            "namespace": "srv",
        |            "name": "sessions-293kc",
        |            "uid": "69f5a7d2-525e-11e5-9859-42010af01815",
        |            "resourceVersion": "4962471"
        |          }
        |        },
        |        {
        |          "ip": "10.248.7.11",
        |          "targetRef": {
        |            "kind": "Pod",
        |            "namespace": "srv",
        |            "name": "sessions-mr9gb",
        |            "uid": "69f5b78e-525e-11e5-9859-42010af01815",
        |            "resourceVersion": "4962524"
        |          }
        |        },
        |        {
        |          "ip": "10.248.8.9",
        |          "targetRef": {
        |            "kind": "Pod",
        |            "namespace": "srv",
        |            "name": "sessions-nicom",
        |            "uid": "69f5b623-525e-11e5-9859-42010af01815",
        |            "resourceVersion": "4962517"
        |          }
        |        }
        |      ],
        |      "ports": [
        |        {
        |          "name": "http",
        |          "port": 8083,
        |          "protocol": "TCP"
        |        }
        |      ]
        |    }
        |  ]
        |}""".stripMargin
    )

    val ScaleUp = Buf.Utf8(
      s"""
        |{
        |  "type": "MODIFIED",
        |  "object": {
        |    "kind": "Endpoints",
        |    "apiVersion": "v1",
        |    "metadata": {
        |      "name": "sessions",
        |      "namespace": "srv",
        |      "selfLink": "$NonWatchPath$SessionsPath",
        |      "uid": "6a698096-525e-11e5-9859-42010af01815",
        |      "resourceVersion": "5319582",
        |      "creationTimestamp": "2015-09-03T17:08:37Z"
        |    },
        |    "subsets": [
        |      {
        |        "addresses": [
        |          {
        |            "ip": "10.248.1.11",
        |            "targetRef": {
        |              "kind": "Pod",
        |              "namespace": "srv",
        |              "name": "sessions-09ujq",
        |              "uid": "669b7a4f-55ef-11e5-a801-42010af08a01",
        |              "resourceVersion": "5319581"
        |            }
        |          },
        |          {
        |            "ip": "10.248.4.9",
        |            "targetRef": {
        |              "kind": "Pod",
        |              "namespace": "srv",
        |              "name": "sessions-293kc",
        |              "uid": "69f5a7d2-525e-11e5-9859-42010af01815",
        |              "resourceVersion": "4962471"
        |            }
        |          },
        |          {
        |            "ip": "10.248.7.11",
        |            "targetRef": {
        |              "kind": "Pod",
        |              "namespace": "srv",
        |              "name": "sessions-mr9gb",
        |              "uid": "69f5b78e-525e-11e5-9859-42010af01815",
        |              "resourceVersion": "4962524"
        |            }
        |          },
        |          {
        |            "ip": "10.248.8.9",
        |            "targetRef": {
        |              "kind": "Pod",
        |              "namespace": "srv",
        |              "name": "sessions-nicom",
        |              "uid": "69f5b623-525e-11e5-9859-42010af01815",
        |              "resourceVersion": "4962517"
        |            }
        |          }
        |        ],
        |        "ports": [
        |          {
        |            "name": "http",
        |            "port": 8083,
        |            "protocol": "TCP"
        |          }
        |        ]
        |      }
        |    ]
        |  }
        |}""".stripMargin
    )

    val ScaleDown = Buf.Utf8(
      s"""
        |{
        |  "type": "MODIFIED",
        |  "object": {
        |    "kind": "Endpoints",
        |    "apiVersion": "v1",
        |    "metadata": {
        |      "name": "sessions",
        |      "namespace": "srv",
        |      "selfLink": "$NonWatchPath$SessionsPath",
        |      "uid": "6a698096-525e-11e5-9859-42010af01815",
        |      "resourceVersion": "5319605",
        |      "creationTimestamp": "2015-09-03T17:08:37Z"
        |    },
        |    "subsets": [
        |      {
        |        "addresses": [
        |          {
        |            "ip": "10.248.4.9",
        |            "targetRef": {
        |              "kind": "Pod",
        |              "namespace": "srv",
        |              "name": "sessions-293kc",
        |              "uid": "69f5a7d2-525e-11e5-9859-42010af01815",
        |              "resourceVersion": "4962471"
        |            }
        |          },
        |          {
        |            "ip": "10.248.7.11",
        |            "targetRef": {
        |              "kind": "Pod",
        |              "namespace": "srv",
        |              "name": "sessions-mr9gb",
        |              "uid": "69f5b78e-525e-11e5-9859-42010af01815",
        |              "resourceVersion": "4962524"
        |            }
        |          },
        |          {
        |            "ip": "10.248.8.9",
        |            "targetRef": {
        |              "kind": "Pod",
        |              "namespace": "srv",
        |              "name": "sessions-nicom",
        |              "uid": "69f5b623-525e-11e5-9859-42010af01815",
        |              "resourceVersion": "4962517"
        |            }
        |          }
        |        ],
        |        "ports": [
        |          {
        |            "name": "http",
        |            "port": 8083,
        |            "protocol": "TCP"
        |          }
        |        ]
        |      }
        |    ]
        |  }
        |}""".stripMargin
    )

    val Auth = Buf.Utf8(
      s"""
        |{
        |  "kind": "Endpoints",
        |  "metadata": {
        |    "name": "auth",
        |    "namespace": "srv",
        |    "selfLink": "${NonWatchPath}namespaces/srv/endpoints/auth",
        |    "uid": "6982a2fc-525e-11e5-9859-42010af01815",
        |    "resourceVersion": "4969546",
        |    "creationTimestamp": "2015-09-03T17:08:35Z"
        |  },
        |  "subsets": [
        |    {
        |      "addresses": [
        |        {
        |          "ip": "10.248.0.10",
        |          "targetRef": {
        |            "kind": "Pod",
        |            "namespace": "srv",
        |            "name": "auth-vmcia",
        |            "uid": "690ef84c-525e-11e5-9859-42010af01815",
        |            "resourceVersion": "4962519"
        |          }
        |        },
        |        {
        |          "ip": "10.248.1.9",
        |          "targetRef": {
        |            "kind": "Pod",
        |            "namespace": "srv",
        |            "name": "auth-d8qnl",
        |            "uid": "690f06c8-525e-11e5-9859-42010af01815",
        |            "resourceVersion": "4962570"
        |          }
        |        },
        |        {
        |          "ip": "10.248.5.9",
        |          "targetRef": {
        |            "kind": "Pod",
        |            "namespace": "srv",
        |            "name": "auth-m79ya",
        |            "uid": "690f05df-525e-11e5-9859-42010af01815",
        |            "resourceVersion": "4962464"
        |          }
        |        }
        |      ],
        |      "ports": [
        |        {
        |          "name": "http",
        |          "port": 8082,
        |          "protocol": "TCP"
        |        }
        |      ]
        |    }
        |  ]
        |}
        |""".stripMargin
    )
    val Projects = Buf.Utf8(
      s"""
        |{
        |  "kind": "Endpoints",
        |  "metadata": {
        |    "name": "projects",
        |    "namespace": "srv",
        |    "selfLink": "${NonWatchPath}namespaces/srv/endpoints/projects",
        |    "uid": "6c39393c-525e-11e5-9859-42010af01815",
        |    "resourceVersion": "4962611",
        |    "creationTimestamp": "2015-09-03T17:08:40Z"
        |  },
        |  "subsets": [
        |    {
        |      "addresses": [
        |        {
        |          "ip": "10.248.0.11",
        |          "targetRef": {
        |            "kind": "Pod",
        |            "namespace": "srv",
        |            "name": "projects-h2zbp",
        |            "uid": "6bc6a899-525e-11e5-9859-42010af01815",
        |            "resourceVersion": "4962606"
        |          }
        |        },
        |        {
        |          "ip": "10.248.7.12",
        |          "targetRef": {
        |            "kind": "Pod",
        |            "namespace": "srv",
        |            "name": "projects-fzfv2",
        |            "uid": "6bc6b7be-525e-11e5-9859-42010af01815",
        |            "resourceVersion": "4962607"
        |          }
        |        },
        |        {
        |          "ip": "10.248.8.10",
        |          "targetRef": {
        |            "kind": "Pod",
        |            "namespace": "srv",
        |            "name": "projects-0o69j",
        |            "uid": "6bc6c27c-525e-11e5-9859-42010af01815",
        |            "resourceVersion": "4962610"
        |          }
        |        }
        |      ],
        |      "ports": [
        |        {
        |          "name": "http",
        |          "port": 8087,
        |          "protocol": "TCP"
        |        }
        |      ]
        |    }
        |  ]
        |}
      """.stripMargin
    )
    val Events = Buf.Utf8(
      s"""
        |{
        |  "kind": "Endpoints",
        |  "metadata": {
        |    "name": "events",
        |    "namespace": "srv",
        |    "selfLink": "${NonWatchPath}namespaces/srv/endpoints/events",
        |    "uid": "67abfc86-525e-11e5-9859-42010af01815",
        |    "resourceVersion": "4962380",
        |    "creationTimestamp": "2015-09-03T17:08:32Z"
        |  },
        |  "subsets": [
        |    {
        |      "addresses": [
        |        {
        |          "ip": "10.248.0.9",
        |          "targetRef": {
        |            "kind": "Pod",
        |            "namespace": "srv",
        |            "name": "events-6g3in",
        |            "uid": "673a6ebf-525e-11e5-9859-42010af01815",
        |            "resourceVersion": "4962378"
        |          }
        |        },
        |        {
        |          "ip": "10.248.5.8",
        |          "targetRef": {
        |            "kind": "Pod",
        |            "namespace": "srv",
        |            "name": "events-l8xyq",
        |            "uid": "673a68fe-525e-11e5-9859-42010af01815",
        |            "resourceVersion": "4962374"
        |          }
        |        },
        |        {
        |          "ip": "10.248.6.8",
        |          "targetRef": {
        |            "kind": "Pod",
        |            "namespace": "srv",
        |            "name": "events-4hkt8",
        |            "uid": "673a664a-525e-11e5-9859-42010af01815",
        |            "resourceVersion": "4962350"
        |          }
        |        }
        |      ],
        |      "ports": [
        |        {
        |          "name": "http",
        |          "port": 8085,
        |          "protocol": "TCP"
        |        }
        |      ]
        |    }
        |  ]
        |}
      """.stripMargin
    )
    val Empty = Buf.Utf8(
      """
        |{
        |  "kind": "Endpoints",
        |  "metadata": {
        |    "name": "empty-subset",
        |    "namespace": "srv",
        |    "selfLink": "/api/v1/namespaces/srv/endpoints/empty-subset",
        |    "uid": "7d5bd683-525e-11e5-9859-42010af01815",
        |    "resourceVersion": "5962619",
        |    "creationTimestamp": "2016-09-03T17:08:41Z"
        |  },
        |  "subsets": null
        |}
      """.stripMargin
    )
    val Inits = Map[String, Buf](
      s"$NonWatchPath$SessionsPath" -> Init,
      s"${NonWatchPath}namespaces/srv/endpoints/auth" -> Auth,
      s"${NonWatchPath}namespaces/srv/empty-subset" -> Empty,
      s"${NonWatchPath}namespaces/srv/endpoints/projects" -> Projects,
      s"${NonWatchPath}namespaces/srv/endpoints/events" -> Events
    )
    val Services = Buf.Utf8("""{"apiVersion":"v1","items":[{"metadata":{"creationTimestamp":"2017-03-24T03:32:27Z","labels":{"name":"sessions"},"name":"sessions","namespace":"srv","resourceVersion":"33186979","selfLink":"/api/v1/namespaces/srv/services/sessions","uid":"8122d7d0-1042-11e7-b340-42010af00004"},"spec":{"clusterIP":"10.199.240.9","ports":[{"name":"http","port":80,"protocol":"TCP","targetPort":54321},{"name":"admin","port":9990,"protocol":"TCP"}],"selector":{"name":"sessions"},"sessionAffinity":"None","type":"LoadBalancer"},"status":{"loadBalancer":{"ingress":[{"ip":"35.184.61.229"}]}}},{"metadata":{"creationTimestamp":"2017-03-24T03:32:27Z","labels":{"name":"projects"},"name":"projects","namespace":"srv","resourceVersion":"33186980","selfLink":"/api/v1/namespaces/srv/services/projects","uid":"8122d7d0-1042-11e7-b340-42010af00005"},"spec":{"clusterIP":"10.199.240.9","ports":[{"name":"http","port":80,"protocol":"TCP","targetPort":54321},{"name":"admin","port":9990,"protocol":"TCP"}],"selector":{"name":"projects"},"sessionAffinity":"None","type":"LoadBalancer"},"status":{"loadBalancer":{}}},{"metadata":{"creationTimestamp":"2017-03-24T03:32:27Z","labels":{"name":"events"},"name":"events","namespace":"srv","resourceVersion":"33186981","selfLink":"/api/v1/namespaces/srv/services/events","uid":"8122d7d0-1042-11e7-b340-42010af00006"},"spec":{"clusterIP":"10.199.240.9","ports":[{"name":"http","port":80,"protocol":"TCP","targetPort":54321},{"name":"admin","port":9990,"protocol":"TCP"}],"selector":{"name":"events"},"sessionAffinity":"None","type":"LoadBalancer"},"status":{"loadBalancer":{"ingress":[{"hostname":"linkerd.io"}]}}},{"metadata":{"creationTimestamp":"2017-03-24T03:32:27Z","labels":{"name":"auth"},"name":"auth","namespace":"srv","resourceVersion":"33186981","selfLink":"/api/v1/namespaces/srv/services/auth","uid":"8122d7d0-1042-11e7-b340-42010af00007"},"spec":{"clusterIP":"10.199.240.10","ports":[{"name":"http","port":80,"protocol":"TCP","targetPort":"http"},{"name":"admin","port":9990,"protocol":"TCP"}],"selector":{"name":"auth"},"sessionAffinity":"None","type":"LoadBalancer"},"status":{"loadBalancer":{"ingress":[{"hostname":"linkerd.io"}]}}}],"kind":"ServiceList","metadata":{"resourceVersion":"33787896","selfLink":"/api/v1/namespaces/srv/services"}}""")
  }

  trait Fixtures {
    @volatile var doInit, didInit, doScaleUp, doScaleDown, doFail = new Promise[Unit]
    @volatile var writer: Writer = null

    val service = Service.mk[Request, Response] {
      case req if Rsps.Inits.contains(req.uri) =>
        val rsp = Response()
        rsp.content = Rsps.Inits(req.uri)
        doInit before Future.value(rsp)
      case req if req.uri == s"$WatchPath$SessionsPath" =>
        val rsp = Response()

        rsp.setChunked(true)

        writer = rsp.writer
        doScaleUp before writer.write(Rsps.ScaleUp) before {
          doScaleDown before writer.write(Rsps.ScaleDown)
        }

        doFail onSuccess { _ =>
          writer.fail(new ChannelClosedException)
        }

        Future.value(rsp)

      case req if req.uri.startsWith(WatchPath) && !req.uri.contains("sessions") =>
        val rsp = Response()
        Future.value(rsp)
      case req if req.uri == s"${WatchPath}{$SessionsPath}resourceVersion=5319582" =>
        val rsp = Response()

        doScaleDown before rsp.writer.write(Rsps.ScaleDown)

        Future.value(rsp)
      case req =>
        // As a workaround for an issue where some tests would enter an
        // infinite retry loop rather than failing, manually throw a
        // `TestFailedException` rather than calling `fail()`.
        //
        // `fail()` may provide slightly more useful information about
        // the failure location, but there was a concurrency issue where
        // the namer would keep retrying infinitely even after `fail()` was
        // called, causing SBT to hang. curiously, this issue doesn't seem
        // to apply when tests are run from IntelliJ?
        throw new TestFailedException(s"unexpected request: $req", 1)
    }
    val api = v1.Api(service)
    val timer = new MockTimer
    val namer = new MultiNsNamer(Path.read("/test"), None, api.withNamespace, Stream.continually(1.millis))(timer)

    def name = "/srv/http/sessions"

    @volatile var stateUpdates: Int = 0
    @volatile var state: Activity.State[NameTree[Name]] = Activity.Pending
    val activity = namer.lookup(Path.read(name))
    val _ = activity.states.respond { s =>
        state = s
        stateUpdates += 1
    }

    def addrs = state match {
      case Activity.Ok(NameTree.Leaf(bound: Name.Bound)) =>
        bound.addr.sample() match {
          case Addr.Bound(addrs, _) =>
            addrs
          case addr =>
            throw new TestFailedException(
              s"expected bound addr, got $addr (after $stateUpdates state updates)", 1
            )
        }
      case v =>
        throw new TestFailedException(s"unexpected state: $v (after $stateUpdates state updates)", 1)
    }

    def assertHas(n: Int) =
      assert(addrs.size == n, s" (after $stateUpdates state updates)")
    def assertUpdates(n: Int) = assert(stateUpdates == n)
  }

  test("single ns namer uses passed in namespace") {
    @volatile var request: Request = null
    @volatile var state: Activity.State[NameTree[Name]] = Activity.Pending

    val service = Service.mk[Request, Response] {
      case req if req.uri == s"$NonWatchPath$SessionsPath" =>
        request = req
        val rsp = Response()
        rsp.content = Rsps.Init
        Future.value(rsp)

      case req if req.uri.startsWith(s"$WatchPath$SessionsPath") =>
        request = req
        val rsp = Response()
        Future.value(rsp)

      case req =>
        throw new TestFailedException(s"unexpected request: $req", 1)
    }

    val api = v1.Api(service)
    val namer = new SingleNsNamer(Path.read("/test"), None, "srv", api.withNamespace, Stream.continually(1.millis))
    namer.lookup(Path.read("/http/sessions/d3adb33f")).states.respond { s =>
      state = s
    }

    assert(
      request.uri.startsWith(s"$WatchPath$SessionsPath"),
      "Request was not sent to correct namespace"
    )
    state match {
      case Activity.Ok(NameTree.Leaf(bound: Name.Bound)) =>
        assert(bound.id == Path.Utf8("test", "http", "sessions"))
      case v =>
        fail(s"unexpected state: $v")
    }
  }

  test("watches a namespace and receives updates") {
    val _ = new Fixtures {
      assert(state == Activity.Pending)
      doInit.setDone()
      assertHas(3)

      doScaleUp.setDone()
      assertHas(4)

      doScaleDown.setDone()
      assertHas(3)
    }
  }

  test("retries initial failures") {
    Time.withCurrentTimeFrozen { time =>
      val _ = new Fixtures {
        assert(state == Activity.Pending)
        doInit.setException(new Exception("should be retried") with NoStackTrace)

        time.advance(1.millis)
        timer.tick()
        assert(state == Activity.Pending)

        doInit = new Promise[Unit]
        doInit.setDone()
        time.advance(1.millis)
        timer.tick()
        assertHas(3)
      }
    }
  }

  test("missing port names are negative") {
    val service = Service.mk[Request, Response] {
      case req if req.uri == "/api/v1/namespaces/srv/endpoints/sessions" =>
        val rsp = Response()
        rsp.content = Rsps.Init
        Future.value(rsp)
      case req if req.uri == "/api/v1/watch/namespaces/srv/endpoints/sessions" =>
        Future.value(Response())
      case req =>
        throw new TestFailedException(s"unexpected request: $req", 1)
    }
    val api = v1.Api(service)
    val namer = new MultiNsNamer(Path.read("/test"), None, api.withNamespace)

    @volatile var state: Activity.State[NameTree[Name]] = Activity.Pending
    val _ = namer.lookup(Path.read("/srv/thrift/sessions")).states respond { s =>
      state = s
    }

    assert(state == Activity.Ok(NameTree.Neg))
  }

  test("reconnects on reader error") {
    val _ = new Fixtures {
      assert(state == Activity.Pending)
      doInit.setDone()
      assertHas(3)

      doScaleUp.setDone()
      assertHas(4)

      doInit = new Promise[Unit]

      doFail.setDone()

      doScaleDown.setDone()
      assertHas(3)
    }
  }

  test("sets labelSelector if not None ") {
    @volatile var req: Request = null

    val service = Service.mk[Request, Response] {
      case r if r.path.startsWith("/api/v1/namespaces/srv/endpoints/sessions") =>
        req = r
        val rsp = Response()
        rsp.content = Rsps.Init
        Future.value(rsp)
      case r if r.path.startsWith("/api/v1/watch/namespaces/srv/endpoints/sessions") =>
        req = r
        val rsp = Response()
        rsp.content = Rsps.Init
        Future.value(rsp)
      case r =>
        throw new TestFailedException(s"unexpected request: $r", 1)
    }

    val api = v1.Api(service)
    val namer = new MultiNsNamer(Path.read("/test"), Some("versionLabel"), api.withNamespace)
    await(namer.lookup(Path.read("/srv/thrift/sessions/d3adb33f")).toFuture)

    assert(req.uri.contains("labelSelector=versionLabel%3Dd3adb33f"))
  }

  test("NameTree doesn't update on endpoint change") {
    val _ = new Fixtures {
      assert(state == Activity.Pending)
      doInit.setDone()
      assertHas(3)
      assert(stateUpdates == 2)

      doScaleUp.setDone()
      assertHas(4)
      assert(stateUpdates == 2)

      doScaleDown.setDone()
      assertHas(3)
      assert(stateUpdates == 2)
    }
  }

  test("namer accepts port numbers") {
    val _ = new Fixtures {

      override def name = "/srv/80/sessions"

      assert(state == Activity.Pending)
      doInit.setDone()

      assert(addrs == Set(Address("10.248.4.9", 54321), Address("10.248.8.9", 54321), Address("10.248.7.11", 54321)))
    }
  }

  test("namer accepts port numbers without target port defined") {
    val _ = new Fixtures {

      override def name = "/srv/9990/sessions"

      assert(state == Activity.Pending)
      doInit.setDone()

      assert(addrs == Set(Address("10.248.4.9", 9990), Address("10.248.8.9", 9990), Address("10.248.7.11", 9990)))
    }
  }

  test("namer accepts port numbers when ingress is not defined") {
    val _ = new Fixtures {

      override def name = "/srv/80/projects"

      assert(state == Activity.Pending)
      doInit.setDone()

      assert(addrs == Set(Address("10.248.0.11", 54321), Address("10.248.7.12", 54321), Address("10.248.8.10", 54321)))
    }
  }

  test("namer accepts port numbers when ingress contains hostname") {
    val _ = new Fixtures {

      override def name = "/srv/80/events"

      assert(state == Activity.Pending)
      doInit.setDone()

      assert(addrs == Set(Address("10.248.0.9", 54321), Address("10.248.5.8", 54321), Address("10.248.6.8", 54321)))
    }
  }

  test("port numbers not defined in service resolve to neg") {
    val _ = new Fixtures {

      override def name = "/srv/555/sessions"

      assert(state == Activity.Pending)
      doInit.setDone()

      assert(state == Activity.Ok(NameTree.Neg))
    }
  }

  test("port numbers can map to named target port") {
    val _ = new Fixtures {

      override def name = "/srv/80/auth"

      assert(state == Activity.Pending)
      doInit.setDone()

      assert(addrs == Set(Address("10.248.0.10", 8082), Address("10.248.1.9", 8082), Address("10.248.5.9", 8082)))
    }
  }

  test("namer is case insensitive") {
    val _ = new Fixtures {
      override def name = "/sRv/HtTp/SeSsIoNs"
      assert(state == Activity.Pending)
      doInit.setDone()
      assertHas(3)

      doScaleUp.setDone()
      assertHas(4)

      doScaleDown.setDone()
      assertHas(3)
    }
  }

  test("namer handles endpoints will null subsets") {
    val _ = new Fixtures {
      override def name = "/srv/http/empty-subset"

      assert(state == Activity.Pending)
      doInit.setDone()

      assert(state == Activity.Ok(NameTree.Neg))
    }
  }
}
