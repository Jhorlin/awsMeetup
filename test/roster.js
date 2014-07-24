/**
 * Created by jhorlin.dearmas on 7/24/2014.
 */
(function(should, supertest, app){
    var request = supertest(app);
   describe("Roster Acceptance Test", function(done){
       it('should add a member',function(done){
            request.post('/members/test@test.com')
                .set('Content-Type', 'application/json')
                .send({
                    first: 'Jhorlin',
                    last: 'De Armas',
                    company: 'riptide',
                    reasons: ["developer"]
                })
                .expect(204)
                .end(function(err, res){
                    done(err);
                });
       });

       it('should add second member',function(done){
           request.post('/members/test2@test.com')
               .set('Content-Type', 'application/json')
               .send({
                   first: 'Jhorlin',
                   last: 'De Armas',
                   company: 'riptide',
                   reasons: ["developer"]
               })
               .expect(204)
               .end(function(err, res){
                   done(err);
               });
       });

       it('should get a member',function(done){
           request.get('/members/test@test.com')
               .set('Content-Type', 'application/json')
               .expect(200)
               .end(function(err, res){
                   done(err);
               });
       });

       it('should get all members', function(done){
           request.get('/members')
               .set('Content-Type', 'application/json')
               .expect(200)
               .end(function(err, res){
                   done();
               });
       });
   });
}(require('should'), require('supertest'), require('../app')));